# splits zip, mbox, eml and pst files into their constituent documents -- mesasges and attachments
# and puts them into a tmp folder
# which is then parsed normally

# why .eml you ask? those aren't archives!
# you're right, but they do contain other files (i.e. attachments)
# so I figure this is the place to handle files that contain other files.

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

          constituent_files.each_with_index do |basename_contents_lambda_attachments_parent, idx|
            basename, contents_lambda, attachments, parent = *basename_contents_lambda_attachments_parent
            tmp_filename = File.join(tmpdir, File.basename(archive_filename), basename )
            FileUtils.mkdir_p(File.dirname(tmp_filename))
            begin
              contents_lambda.call(tmp_filename)
            rescue Errno::ENOENT
              puts "#{tmp_filename} wasn't extracted from #{archive_filename}"
              next
            end
            attachments ||= []
            yielder.yield tmp_filename, File.join(File.basename(archive_filename), basename), attachments, parent
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
          begin
            while mail = folder.getNextChild

              # TODO: there exist some objects called EnterpriseVault Shortcuts in some PSTs
              # PSTFile doesn't know how to parse them and will complain:
              # Unknown message type: IPM.Note.EnterpriseVault.Shortcut
              # in practice, the body gets extracted, but not any headers (incl. To/From/Subj)
              # or attachments.
              # if we detect one of these EnterpriseVault Shortcuts objects
              # we'll create "fake" EML headers for it, to make a fake EML

              headers = mail.get_transport_message_headers
              if mail.get_transport_message_headers.strip.empty?
                # mail.java_send(:getItems).to_a.each{|f| puts f.inspect }
                subject = mail.java_send :getSubject
                recip = '"' + mail.get_string_item(0x0E04).split(";").join('"; "') + '"'
                sender = mail.get_string_item(0x5D01)
                time = begin 
                        DateTime.parse(mail.get_date_item(0x3007).to_s.strip).strftime("%a, %d %b %Y %H:%M:%S %z")
                       rescue
                         nil
                       end
                headers = ["Received: fake", "Subject: #{subject}", "To: #{recip}", "From: #{sender}", time.nil? ? nil : "Date: #{time}"].compact.join("\n") + "\n\n"
              end

              # creating a simple EML version of the email, so Tika can read it (in the next step, in stevedore-uploader.rb)
              eml_str = headers + mail.get_body


              # first we handle and yield the attachments
              # then yield the containing EML
              # so that we can yield a list of the filenames of the attachments along with the containing EML
              attachment_basenames = []
              attachment_count = mail.get_number_of_attachments
              attachment_count.times do |attachment_idx|
                attachment = mail.get_attachment(attachment_idx)
                attachment_filename = attachment.get_filename
                begin
                  attachment_basenames << "#{idx}-#{attachment_filename}"
                  yielder << ["#{idx}-#{attachment_filename}", lambda {|fn| open(fn, 'wb'){ |fh| fh << attachment.get_file_input_stream.to_io.read }}]
                rescue java.lang.NullPointerException,java.lang.ArrayIndexOutOfBoundsException
                  next
                end
              end
              yielder << ["#{idx}.eml", lambda{|fn| open(fn, 'wb'){|fh| fh << eml_str } }, attachment_basenames]
              idx += 1
            end

          rescue java.lang.ArrayIndexOutOfBoundsException => e
            # I think it's just the end of hte folder
            next
          end
        end
      end
    end

    def self.get_attachments_from_eml(email_filename)
      Enumerator.new do |yielder|
        mail = Mail.new open(email_filename){|f| f.read }
        attachment_results = mail.attachments.map do |attachment|
          [attachment.filename, lambda{|fn| open(fn, 'wb'){|fh| fh << attachment.body.decoded }}, [], File.basename(email_filename)]
        end
        attachment_basenames = attachment_results.map{|a| File.basename(a[0]) }
        yielder << [File.basename(email_filename), lambda{|fn| open(fn, 'wb'){|fh| fh << open(email_filename){|f| f.read } } }, attachment_basenames, nil]
        attachment_results.each{|res| yielder << res }
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
            # TODO copy over stuff from get_attachments_from_eml for attachment/parents if 
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
