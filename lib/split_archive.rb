# splits zip, mbox, pst files into their constituent documents -- messages and attachments
# and puts them into a tmp folder
# which is then parsed normally

require 'tmpdir'
require 'mail'
require 'zip'
require 'pst' # for PST files


# splits PST and Mbox formats
module Stevedore
  class ArchiveSplitter
    HANDLED_FORMATS = ["zip", "mbox", "pst", "eml"]

    def self.split(archive_filename)
      # if it's a PST use split_pst
      # if it's an mbox, use split_mbox, etc.
      # return a list of files
      Enumerator.new do |yielder|
        Dir.mktmpdir do |tmpdir|
          #TODO should probably do magic byte searching etc.
          extension = archive_filename.split(".")[-1]
          puts "splitting #{archive_filename}"
          constituent_files =  if extension == "mbox"
                          self.split_mbox(archive_filename)
                        elsif extension == "pst"
                          self.split_pst(archive_filename)
                        elsif extension == "zip"
                          self.split_zip(archive_filename)
                        elsif extension == "eml"
                          self.get_attachments_from_eml(archive_filename)                                                    
                        end
          # should yield a relative filename
          # and a lambda that will write the file contents to the given filename
          FileUtils.mkdir_p(File.join(tmpdir, File.basename(archive_filename)))

          constituent_files.each_with_index do |basename_contents_lambda, idx|
            basename, contents_lambda = *basename_contents_lambda
            tmp_filename = File.join(tmpdir, File.basename(archive_filename), basename.gsub("/", "") )
            FileUtils.mkdir_p(File.dirname(tmp_filename))
            begin
              contents_lambda.call(tmp_filename)
            rescue Errno::ENOENT
              puts "#{tmp_filename} wasn't extracted from #{archive_filename}" 
              next
            end
            yielder.yield tmp_filename, File.join(File.basename(archive_filename), basename)             
          end
        end
      end
    end

    def self.split_pst(archive_filename)
      pstfile = Java::ComPFF::PSTFile.new(archive_filename)
      idx = 0
      folders = pstfile.root.sub_folders.inject({}) do |memo,f|
        memo[f.name] = f
        memo
      end
      Enumerator.new do |yielder|
        folders.each do |folder_name, folder|
          while mail = folder.getNextChild

            eml_str = mail.get_transport_message_headers + mail.get_body

            yielder << ["#{idx}.eml", lambda{|fn| open(fn, 'wb'){|fh| fh << eml_str } }]
            attachment_count = mail.get_number_of_attachments
            attachment_count.times do |attachment_idx|
              attachment = mail.get_attachment(attachment_idx)
              attachment_filename = attachment.get_filename
              yielder << ["#{idx}-#{attachment_filename}", lambda {|fn| open(fn, 'wb'){ |fh| fh << attachment.get_file_input_stream.to_io.read }}]
            end
            idx += 1
          end
        end
      end
    end

    def self.split_mbox(archive_filename)
      # stolen shamelessly from the Ruby Enumerable docs, actually
      # split mails in mbox (slice before Unix From line after an empty line)
      Enumerator.new do |yielder|
        open(archive_filename) do |fh|
          fh.slice_before(empty: true) do |line, h|
            previous_was_empty = h[:empty]
            h[:empty] = line == "\n" || line == "\r\n" || line == "\r"
            previous_was_empty && line.start_with?("From ")
          end.each_with_index do |mail_str, idx| 
            mail_str.pop if mail_str.last == "\n" # remove last line if prexent
            yielder << ["#{idx}.eml", lambda{|fn| open(fn, 'wb'){|fh| fh << mail_str.join("") } }]
            mail = Mail.new mail_str.join("")
            mail.attachments.each do |attachment|
              yielder << [attachment.filename, lambda{|fn| open(fn, 'wb'){|fh| fh << attachment.body.decoded }}]
            end
          end
        end
      end
    end

    def self.get_attachments_from_eml(email_filename)
      Enumerator.new do |yielder|
        yielder << [File.basename(email_filename), lambda{|fn| open(fn, 'wb'){|fh| fh << open(email_filename){|f| f.read } } }]
        mail = Mail.new open(email_filename){|f| f.read }
        mail.attachments.each do |attachment|
          yielder << [attachment.filename, lambda{|fn| open(fn, 'wb'){|fh| fh << attachment.body.decoded }}]
        end
      end
    end


    def self.split_zip(archive_filename)
      Zip::File.open(archive_filename) do |zip_file|
        Enumerator.new do |yielder|
          zip_file.each do |entry|
           begin
             yielder << [entry.name, lambda{|fn| entry.extract(fn) }]
           rescue
             puts "unable to extract #{entry.name} from #{archive_filename}"
           end             
          end
        end
      end
    end

  end
end
