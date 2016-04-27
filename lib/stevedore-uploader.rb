Dir["#{File.expand_path(File.dirname(__FILE__))}/../lib/*.rb"].each {|f| require f}
Dir["#{File.expand_path(File.dirname(__FILE__))}/../lib/parsers/*.rb"].each {|f| require f}

require 'rika'

require 'net/https'
require 'elasticsearch'
require 'elasticsearch/transport/transport/http/manticore'
require 'net/https'

require 'manticore'
require 'fileutils'
require 'csv'


require 'aws-sdk'


module Stevedore
  class ESUploader
    #creates blobs
    attr_reader :errors
    attr_accessor :should_ocr, :slice_size, :client

    def initialize(es_host, es_index, s3_bucket=nil, s3_path=nil)
      @errors = []  
      @client = Elasticsearch::Client.new({
          log: false,
          url: es_host,
          transport_class: Elasticsearch::Transport::Transport::HTTP::Manticore,
          request_timeout: 5*60,
          socket_timeout: 60
        },
      )
      @es_index = es_index
      @s3_bucket = s3_bucket #|| (Stevedore::ESUploader.const_defined?(FOLDER) && FOLDER.downcase.include?('s3://') ? FOLDER.gsub(/s3:\/\//i, '').split("/", 2).first : nil)
      @s3_basepath = "https://#{s3_bucket}.s3.amazonaws.com/#{s3_path || es_index}"

      @slice_size =  100

      @should_ocr = false

      self.create_index!
      self.add_mapping(:doc, MAPPING)
    end

    def create_index!
      begin
        @client.indices.create(
          index: @es_index, 
          body: {
            settings: {
              analysis: {
                analyzer: {
                  email_analyzer: {
                    type: "custom",
                    tokenizer: "email_tokenizer",
                    filter: ["lowercase"]
                  },
                  snowball_analyzer: {
                    type: "snowball",
                    language: "English"
                  }

                },
                tokenizer: {
                  email_tokenizer: {
                    type: "pattern",
                    pattern: "([a-zA-Z0-9_\\.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-\\.]+)",
                    group: "0"
                  }
                }
              }
            },
          }) 
      # don't complain if the index already exists.
      rescue Elasticsearch::Transport::Transport::Errors::BadRequest => e
        raise e unless e.message && (e.message.include?("IndexAlreadyExistsException") || e.message.include?("already exists as alias"))
      end
    end

    def add_mapping(type, mapping)
      @client.indices.put_mapping({
        index: @es_index,
        type: type,
        body: {
          "_id" => {
            path: "id"
          },          
          properties: mapping
        }
      }) # was "rescue nil" but that obscured meaningful errors
    end

    def bulk_upload_to_es!(data, type=nil)
      return nil if data.empty?
      begin
        resp = @client.bulk body: data.map{|datum| {index: {_index: @es_index, _type: type || 'doc', data: datum }} }
        puts resp if resp[:errors]
      rescue JSON::GeneratorError
        data.each do |datum|
          begin
            @client.bulk body: [datum].map{|datum| {index: {_index: @es_index, _type: type || 'doc', data: datum }} }
          rescue JSON::GeneratorError
            next
          end
        end
        resp = nil
      end
      resp
    end

    def process_document(filename, filename_for_s3)
      begin
        puts "begin to process #{filename}"
        # puts "size: #{File.size(filename)}"
        begin
          content, metadata = Rika.parse_content_and_metadata(filename)
        rescue StandardError
          content = "couldn't be parsed"
          metadata = "couldn't be parsed"
        end
        puts "parsed: #{content.size}"
        if content.size > 10 * (10 ** 6)
          @errors << filename
          puts "skipping #{filename} for being too big"
          return nil
        end
        puts metadata["Content-Type"].inspect

        # TODO: factor these out in favor of the yield/block situation down below.
        # this should (eventually) be totally generic, but perhaps handle common 
        # document types on its own
        ret = case                             # .eml                                          # .msg
              when metadata["Content-Type"] == "message/rfc822" || metadata["Content-Type"] == "application/vnd.ms-outlook"
                ::Stevedore::StevedoreEmail.new_from_tika(content, metadata, filename_for_s3, filename).to_hash
              when metadata["Content-Type"] && ["application/html", "application/xhtml+xml"].include?(metadata["Content-Type"].split(";").first)
                ::Stevedore::StevedoreHTML.new_from_tika(content, metadata, filename_for_s3, filename).to_hash
              when @should_ocr && metadata["Content-Type"] == "application/pdf" && (content.match(/\A\s*\z/) || content.size < 50 * metadata["xmpTPg:NPages"].to_i )
                # this is a scanned PDF.
                puts "scanned PDF #{File.basename(filename)} detected; OCRing"
                pdf_basename = filename.gsub(".pdf", '')
                system("convert","-monochrome","-density","300x300",filename,"-depth",'8',"#{pdf_basename}.png")
                (Dir["#{pdf_basename}-*.png"] + Dir["#{pdf_basename}.png"]).sort_by{|png| (matchdata = png.match(/-\d+\.png/)).nil? ? 0 : matchdata[0].to_i }.each do |png|
                  system('tesseract', png, png, "pdf")
                  File.delete(png)
                  # no need to use a system call when we could use the stdlib!
                  # system("rm", "-f", png) rescue nil
                  File.delete("#{png}.txt")
                end.join("\n\n")
                # e.g.  Analysis-Corporation-2.png.pdf or Torture.pdf
                files = Dir["#{pdf_basename}.png.pdf"] + (Dir["#{pdf_basename}-*.png.pdf"].sort_by{|pdf| Regexp.new("#{pdf_basename}-([0-9]+).png.pdf").match(pdf)[1].to_i })
                return nil if files.empty?
                system('pdftk', *files, "cat", "output", "#{pdf_basename}.ocr.pdf")
                content, _ = Rika.parse_content_and_metadata("#{pdf_basename}.ocr.pdf")
                puts "OCRed content (#{File.basename(filename)}) length: #{content.length}"
                ::Stevedore::StevedoreBlob.new_from_tika(content, metadata, filename_for_s3, filename).to_hash
              else
                ::Stevedore::StevedoreBlob.new_from_tika(content, metadata, filename_for_s3, filename).to_hash
              end
      [ret, content, metadata]
      rescue StandardError, java.lang.NoClassDefFoundError, org.apache.tika.exception.TikaException => e
        STDERR.puts e.inspect
        STDERR.puts "#{e} #{e.message}: #{filename}"
        STDERR.puts e.backtrace.join("\n") + "\n\n\n"
        # puts "\n"
        @errors << filename
        nil
      end
    end

    def do_csv!(file, download_url, title_column=0, text_column=nil)
      docs_so_far = 0
      CSV.open(file, headers: (!title_column.is_a? Fixnum ) ).each_slice(@slice_size).each_with_index do |slice, slice_index|
        slice_of_rows = slice.map.each_with_index do |row, i|
          doc = ::Stevedore::StevedoreCsvRow.new(
            row[title_column], 
            (row.respond_to?(:to_hash) ? (text_column.nil? ? row.to_hash.each_pair.map{|k, v| "#{k}: #{v}"}.join(" \n\n ") : row[text_column]) : row.to_a.join(" \n\n ")) + " \n\n csv_source: #{File.basename(file)}", 
            (@slice_size * slice_index )+ i, 
            download_url, 
            row).to_hash
          doc["analyzed"] ||= {}
          doc["analyzed"]["metadata"] ||= {}
          yield doc if block_given? && doc
          doc
        end
        begin
          resp = bulk_upload_to_es!(slice_of_rows.compact)
          docs_so_far += @slice_size
        rescue Manticore::Timeout, Manticore::SocketException
          STDERR.puts("retrying at #{Time.now}")
          retry
        end
        puts "uploaded #{slice_of_rows.size} rows to #{@es_index}; #{docs_so_far} uploaded so far"
        puts "Errors in bulk upload: #{resp.inspect}" if resp && resp["errors"]
      end
    end

    def do!(target_path, output_stream=STDOUT)
      output_stream.puts "Processing documents from #{target_path}"

      docs_so_far = 0
      use_s3 = false # option to set this (an option to set document URLs to be relative to the search engine root) is TK
      @s3_bucket =  target_path.gsub(/s3:\/\//i, '').split("/", 2).first if @s3_bucket.nil? && target_path.downcase.include?('s3://')

      if target_path.downcase.include?("s3://")
        Dir.mktmpdir do |dir|
          Aws.config.update({
            region: 'us-east-1', # TODO should be configurable
          })
          s3 = Aws::S3::Resource.new

          bucket = s3.bucket(@s3_bucket)
          s3_path_without_bucket = target_path.gsub(/s3:\/\//i, '').split("/", 2).last
          bucket.objects(:prefix => s3_path_without_bucket).each_slice(@slice_size) do |slice_of_objs|
            docs_so_far += slice_of_objs.size

            output_stream.puts "starting a set of #{@slice_size} -- so far #{docs_so_far}"
            slice_of_objs.map! do |obj|
              next if obj.key[-1] == "/"
              FileUtils.mkdir_p(File.join(dir, File.dirname(obj.key))) 
              tmp_filename = File.join(dir, obj.key)
              begin
                body = obj.get.body.read
                File.open(tmp_filename, 'wb'){|f| f << body}
              rescue Aws::S3::Errors::NoSuchKey
                @errors << obj.key
              rescue ArgumentError
                File.open(tmp_filename, 'wb'){|f| f << body.nil? ? '' : body.chars.select(&:valid_encoding?).join}
              end
              download_filename = "https://#{@s3_bucket}.s3.amazonaws.com/" + obj.key

              # is this file an archive that contains a bunch of documents we should index separately?
              # obviously, there is not a strict definition here.
              # emails in mailboxes are split into an email and attachments
              # but, for now, standalone emails are treated as one document
              # PDFs can (theoretically) contain documents as "attachments" -- those aren't handled here either.x
              if ArchiveSplitter::HANDLED_FORMATS.include?(tmp_filename.split(".")[-1]) 
                ArchiveSplitter.split(tmp_filename).map do |constituent_file, constituent_basename|
                  doc, content, metadata = process_document(constituent_file, download_filename)
                  doc["sha1"] = Digest::SHA1.hexdigest(download_filename + File.basename(constituent_basename)) # since these files all share a download URL (that of the archive, we need to come up with a custom sha1)
                  yield doc, obj.key, content, metadata if block_given?
                  FileUtils.rm(constituent_file) rescue Errno::ENOENT # try to delete, but no biggie if it doesn't work for some weird reason.
                  doc
                end
              else 
                doc, content, metadata = process_document(tmp_filename, download_filename)
                yield doc, obj.key, content, metadata if block_given?
                FileUtils.rm(tmp_filename) rescue Errno::ENOENT # try to delete, but no biggie if it doesn't work for some weird reason.
                [doc]
              end
            end
            begin
              resp = bulk_upload_to_es!(slice_of_objs.compact.flatten(1)) # flatten, in case there's an archive
              puts resp.inspect if resp && resp["errors"]
            rescue Manticore::Timeout, Manticore::SocketException
              output_stream.puts("retrying at #{Time.now}")
              retry
            end
            output_stream.puts "uploaded #{slice_of_objs.size} files to #{@es_index}; #{docs_so_far} uploaded so far"
            output_stream.puts "Errors in bulk upload: #{resp.inspect}" if resp && resp["errors"]
          end
        end
      else
        list_of_files = File.file?(target_path) ? [target_path] : Dir[File.join(target_path, target_path.include?('*') ? '' : '**/*')]
        list_of_files.each_slice(@slice_size) do |slice_of_files|
          output_stream.puts "starting a set of #{@slice_size}"
          docs_so_far += slice_of_files.size

          slice_of_files.map! do |filename|
            next unless File.file?(filename)
            filename_basepath = filename.gsub(target_path, '')
            if use_s3
              download_filename = @s3_basepath + filename_basepath
            else
              download_filename = "/files/#{@es_index}/#{filename_basepath}"
            end

            # is this file an archive that contains a bunch of documents we should index separately?
            # obviously, there is not a strict definition here.
            # emails in mailboxes are split into an email and attachments
            # but, for now, standalone emails are treated as one document
            # PDFs can (theoretically) contain documents as "attachments" -- those aren't handled here either.
            if ArchiveSplitter::HANDLED_FORMATS.include?(filename.split(".")[-1]) 
              ArchiveSplitter.split(filename).map do |constituent_file, constituent_basename|
                doc, content, metadata = process_document(constituent_file, download_filename)
                doc["sha1"] = Digest::SHA1.hexdigest(download_filename + File.basename(constituent_basename)) # since these files all share a download URL (that of the archive, we need to come up with a custom sha1)
                yield doc, filename, content, metadata if block_given?
                # FileUtils.rm(constituent_file) rescue Errno::ENOENT # try to delete, but no biggie if it doesn't work for some weird reason.
                doc
              end
            else
              doc, content, metadata = process_document(filename, download_filename  )
              yield doc, filename, content, metadata if block_given?
              [doc]
            end
          end
          begin
            resp = bulk_upload_to_es!(slice_of_files.compact.flatten(1)) # flatten, in case there's an archive
            puts resp.inspect if resp && resp["errors"]
          rescue Manticore::Timeout, Manticore::SocketException => e
            output_stream.puts e.inspect
            output_stream.puts "Upload error: #{e} #{e.message}."
            output_stream.puts e.backtrace.join("\n") + "\n\n\n"
            output_stream.puts("retrying at #{Time.now}")
            retry
          end
          output_stream.puts "uploaded #{slice_of_files.size} files to #{@es_index}; #{docs_so_far} uploaded so far"
          output_stream.puts "Errors in bulk upload: #{resp.inspect}" if resp && resp["errors"]
        end
      end
    end
  end
  MAPPING = {
              sha1: {type: :string, index: :not_analyzed},
              title: { type: :string, analyzer: :keyword },
              source_url: {type: :string, index: :not_analyzed},
              modifiedDate: { type: :date, format: "dateOptionalTime" },
              _updated_at: { type: :date },
              analyzed: {
                properties: {
                  body: {
                    type: :string, 
                    index_options: :offsets, 
                    term_vector: :with_positions_offsets,
                    store: true,
                    fields: {
                      snowball: {
                        type: :string,
                        index: "analyzed",
                        analyzer: 'snowball_analyzer' ,
                        index_options: :offsets, 
                        term_vector: :with_positions_offsets,
                      }
                    }
                  },
                  metadata: {
                    properties: {
                      # "attachments" =>  {type: :string, index: :not_analyzed}, # might break stuff; intended to keep the index name (which often contains relevant search terms) from being indexed, e.g. if a user wants to search for 'bernie' in the bernie-burlington-emails
                      "Message-From" => {
                        type: "string",
                        fields: {
                          email: {
                            type: "string",
                            analyzer: "email_analyzer"
                          },
                          "Message-From" => {
                            type: "string"
                          }
                        }
                      },
                      "Message-To" => {
                        type: "string",
                        fields: {
                          email: {
                            type: "string",
                            analyzer: "email_analyzer"
                          },
                          "Message-To" => {
                            type: "string"
                          }
                        }
                      }                  
                    }
                  }
                }
              }
            }
end
