require_relative './stevedore_blob'
require 'cgi'
require 'digest/sha1'
require 'manticore'
require 'dkimverify'


module Stevedore
  class StevedoreEmail < StevedoreBlob


    # TODO write wrt other fields. where do those go???
    attr_accessor :creation_date, :message_to, :message_from, :message_cc, :subject, :attachments, :content_type, :dkim_verified

    def self.new_from_tika(content, metadata, download_url, filepath)
      t = super
      t.creation_date = metadata["Creation-Date"]
      t.message_to = metadata["Message-To"]
      t.message_from = metadata["Message-From"]
      t.message_cc = metadata["Message-Cc"]
      t.title = t.subject = metadata["subject"]
      t.dkim_verified = filepath.end_with?("eml") && begin 
                          DkimVerify::Verification::Verifier.new(open(filepath, 'r'){|f| f.read }).verify!
                        rescue DkimVerify::Verification::DkimError, DkimVerify::Mail::MessageFormatError
                          false
                        end
      t.attachments = metadata["X-Attachments"].to_s.split("|").map do |raw_attachment_filename| 
        attachment_filename = CGI::unescape(raw_attachment_filename)
        possible_filename = File.join(File.dirname(filepath), attachment_filename)
        eml_filename = File.join(File.dirname(filepath), File.basename(filepath, '.eml') + '-' + attachment_filename)
        possible_s3_url = S3_BASEPATH + '/' + CGI::escape(File.basename(possible_filename))
        possible_eml_s3_url = S3_BASEPATH + '/' + CGI::escape(File.basename(eml_filename))

        # we might be uploading from the disk in which case we see if we can find an attachment on disk with the name from X-Attachments
        # or we might be uploading via S3, in which case we see if an object exists, accessible on S3, with the path from X-Attachments
        # TODO: support private S3 buckets
        s3_url = if File.exists? possible_filename
                    possible_s3_url
                 elsif File.exists? eml_filename
                    possible_eml_s3_url
                 else
                    nil
                 end
        s3_url = begin
                  if Manticore::Client.new.head(possible_s3_url).code == 200
                    puts "found attachment: #{possible_s3_url}"
                    possible_s3_url
                  elsif Manticore::Client.new.head(possible_eml_s3_url).code == 200
                    puts "found attachment: #{possible_eml_s3_url}"
                    possible_eml_s3_url
                  end
                rescue
                  nil
                end if s3_url.nil?
        if s3_url.nil?
          STDERR.puts "Tika X-Attachments: " + metadata["X-Attachments"].to_s.inspect
          STDERR.puts "Couldn't find attachment '#{possible_s3_url}' aka '#{possible_eml_s3_url}' from '#{raw_attachment_filename}' from #{download_url}"
        end
        s3_url
      end.compact
      t
    end


    def to_hash
      {
        "sha1" => Digest::SHA1.hexdigest(download_url),
        "title" => title.to_s,
        "source_url" => download_url.to_s,
        "file" => {
          "title" => title.to_s,
          "file" => text.to_s
        },
        "analyzed" => {
          "body" => text.to_s,
          "metadata" => {
            "Content-Type" => content_type || "message/rfc822",
            "Creation-Date" => creation_date,
            "Message-To" => message_from.is_a?(Enumerable) ? message_to : [ message_to ],
            "Message-From" => message_from.is_a?(Enumerable) ? message_from : [ message_from ],
            "Message-Cc" => message_cc.is_a?(Enumerable) ? message_cc : [ message_cc ],
            "subject" => subject,
            "attachments" => attachments,
            "dkim_verified" => dkim_verified
          }
        },
        "_updatedAt" => Time.now
      }
    end

  end
end
