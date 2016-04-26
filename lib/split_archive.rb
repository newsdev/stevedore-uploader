# splits zip, mbox and pst files into their constituent documents -- mesages and attachments
# and puts them into a tmp folder
# which is then parsed normally
require 'mapi/msg'
require 'tmpdir'
require 'mapi/pst'
require 'mail'
require 'zip'

# splits PST and Mbox formats
module Stevedore
  class ArchiveSplitter
    def self.split(archive_filename)
      # if it's a PST use split_pst
      # if it's an mbox, use split_pst
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
                        end
          # should yield a relative filename
          # and a lambda that will write the file contents to the given filename
          FileUtils.mkdir_p(File.join(tmpdir, File.basename(archive_filename)))

          constituent_files.each_with_index do |basename_contents_lambda, idx|
            basename, contents_lambda = *basename_contents_lambda
            tmp_filename = File.join(tmpdir, File.basename(archive_filename), basename )
            contents_lambda.call(tmp_filename)
            yielder.yield tmp_filename, File.join(File.basename(archive_filename), basename)
          end
        end
      end
    end

    def self.split_pst(archive_filename)
      pst = Mapi::Pst.new open(archive_filename)
      Enumerator.new do |yielder|
        pst.each_with_index do |mail, idx|
          msg = Mapi::Msg.load mail
          yielder << ["#{idx}.eml", lambda{|fn| open(fn, 'wb'){|fh| fh << mail } }]
          msg.attachments.each do |attachment|
            yielder << [attachment.filename, lambda{|fn| open(fn, 'wb'){|fh| attachment.save fh }}]
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
              yielder << [attachment.filename, lambda{|fn| open(fn, 'wb'){|fh| attachment.save fh }}]
            end
          end
        end
      end
    end

    def self.split_zip(archive_filename)
      Zip::File.open(archive_filename) do |zip_file|
        Enumerator.new do |yielder|
          zip_file.each do |entry|
            yielder << [entry.name, lambda{|fn| entry.extract(fn) }]
          end
        end
      end
    end

  end
end
