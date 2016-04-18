# splits zip, mbox and pst files into their constituent documents -- mesages and attachments
# and puts them into a tmp folder
# which is then parsed normally
require 'mapi/msg'
require 'tmpdir'
require 'mapi/pst'
require 'zip'

# splits PST and Mbox formats

class ArchiveSplitter
  def self.split(archive_filename)
    # if it's a PST use split_pst
    # if it's an mbox, use split_pst
    # return a list of files
    Dir.mktmpdir do |dir|
      #TODO should probably do magic byte searching et.c
      extension = dir.archive_filename.split(".")[-1]

      constituent_files =  if extension == "mbox"
                      self.split_mbox(archive_filename)
                    elsif extension == "pst"
                      self.split_pst(archive_filename)
                    elsif extension == "zip"
                      self.split_zip(archive_filename)
                    end
      # should yield a relative filename
      # and a lambda that will write the file contents to the given filename

      constituent_files.each do |filename, contents_lambda|
        contents_lambda.call(File.join(dir, File.basename(archive_filename), filename ))
      end
    end    
  end
end

class MailArchiveSplitter

  def self.split_pst(archive_filename)
    pst = Mapi::Pst.new open(archive_filename)
    pst.each_with_index do |mail, idx|
      msg = Mapi::Msg.load mail
      yield "#{idx}.eml", lambda{|fn| open(fn, 'wb'){|fh| fh << mail } }
      msg.attachments.each do |attachment|
        yield attachment.filename, lambda{|fn| open(fn, 'wb'){|fh| attachment.save fh }}
      end
    end

  end

  def self.split_mbox(archive_filename)
    # stolen shamelessly from the Ruby Enumerable docs, actually
    # split mails in mbox (slice before Unix From line after an empty line)
    open(archive_filename) do |fh|
      f.slice_before(empty: true) do |line, h|
        previous_was_empty = h[:empty]
        h[:empty] = line == "\n"
        previous_was_empty && line.start_with?("From ")
      end.each_with_index do |mail, idx| 
        mail.pop if mail.last == "\n" # remove last line if prexent
        yield "#{idx}.eml", lambda{|fn| open(fn, 'wb'){|fh| f << mail } }
        msg.attachments.each do |attachment|
          yield attachment.filename, lambda{|fn| open(fn, 'wb'){|fh| attachment.save f }}
        end
      end
    end
  end

  def self.split_zip(archive_filename)
    Zip::File.open(archive_filename) do |zip_file|
      zip_file.each do |entry|
        yield entry.names, lambda{|fn| entryhextract(fn) }
      end
    end
  end

end
