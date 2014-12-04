module Longupload::StoresInWarehouse
  def self.included(mod)
    mod.instance_eval do
      serialize :warehouse_blocks, Array
    end
  end

  def after_longupload_block(block_index)
    todo_file = "#{longupload_cachefile}.todo.#{block_index}"
    if !File.exists? todo_file
      # Whoever deleted that todo file should have made sure
      # self.warehouse_blocks was updated first.  If so, there's
      # nothing for us to do.
      reload                    # in case of race
      if self.warehouse_blocks and self.warehouse_blocks[block_index]
        return self.warehouse_blocks[block_index]
      end
      raise "Oops, #{self.class} #{self.id} block #{block_index} not found in staging area."
    end

    Open3.popen3('arv-put', '--raw', todo_file) do |std_in, std_out, std_err, wait_thr|
      block_locator = std_out.gets.strip rescue nil
      if wait_thr.respond_to? :value
        # ruby 1.9.3 actual exit status
        exitvalue = wait_thr.value
      else
        # ruby 1.8.7 guess exit status based on output of arv-put
        exitvalue = (block_locator && block_locator.match(/^[\da-f]{32}\b/) ? 0 : -1)
      end
      if block_locator and !block_locator.empty? and exitvalue == 0
        self.class.transaction do
          reload
          self.warehouse_blocks ||= []
          self.warehouse_blocks[block_index] = block_locator
          save!
        end
        File.unlink todo_file
      else
        logger.error "longupload #{self.class} #{self.id} arv-put block #{block_index} exited #{exitvalue}: #{std_err.gets}"
        raise "Block write failed"
      end
    end
  end

  def after_longupload_file
    app_name = Rails.application.class.to_s.split('::').first
    Open3.popen3('python', '-c', <<EOS,
import arvados
import sys
print (arvados.api('v1').
       collections().
       create(body={'manifest_text': sys.stdin.read(), 'name': sys.argv[-1]}).
       execute()['portable_data_hash'])
EOS
                 "#{app_name}--#{ROOT_URL}--#{self.class}--#{self.id}"
                 ) do |std_in, std_out, std_err, wait_thr|
      std_in.puts ". #{self.warehouse_blocks.join ' '} 0:#{self.longupload_size}:#{self.longupload_file_name.gsub ' ', '_'}"
      std_in.close
      new_manifest_locator = std_out.gets.strip rescue nil
      if wait_thr.respond_to? :value
        # ruby 1.9.3 actual exit status
        exitvalue = wait_thr.value
      else
        # ruby 1.8.7 guess exit status based on output
        exitvalue = (new_manifest_locator && new_manifest_locator.match(/^[\da-f]{32}\b/) ? 0 : -1)
      end
      if new_manifest_locator and !new_manifest_locator.empty? and exitvalue == 0
        self.class.transaction do
          reload
          self.warehouse_manifest_locator = new_manifest_locator
          save!
        end
      else
        logger.error "longupload #{self.class} #{self.id} collections.create exited #{exitvalue}: #{std_err.gets}"
        raise "Collection write failed"
      end
    end
  end
end
