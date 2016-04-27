#!/usr/bin/env jruby
# -*- coding: utf-8 -*-

raise Exception, "You've gotta use JRuby" unless RUBY_PLATFORM == 'java'
raise Exception, "You've gotta use Java 1.8; you're on #{java.lang.System.getProperties["java.runtime.version"]}" unless java.lang.System.getProperties["java.runtime.version"] =~ /1\.8/

require "#{File.expand_path(File.dirname(__FILE__))}/../lib/stevedore-uploader.rb"

if __FILE__ == $0
  require 'optparse'
  require 'ostruct'
  options = OpenStruct.new
  options.ocr = true

  op = OptionParser.new("Usage: upload_to_elasticsearch [options] target_(dir_or_csv)") do |opts|
    opts.on("-hSERVER:PORT", "--host=SERVER:PORT",
            "The location of the ElasticSearch server") do |host|
      options.host = host
    end

    opts.on("-iNAME", "--index=NAME",
            "A name to use for the ES index (defaults to using the directory name)") do |index|
      options.index = index
    end

    opts.on("-sPATH", "--s3path=PATH",
            "The path under your bucket where these files have been uploaded. (defaults to ES index)"
      ) do |s3path|
      options.s3path = s3path
    end
    opts.on("-bPATH", "--s3bucket=PATH",
            "The s3 bucket where these files have already been be uploaded (or will be later)."
      ) do |s3bucket|
      options.s3bucket = s3bucket
    end

    opts.on("--title_column=COLNAME",
            "If target file is a CSV, which column contains the title of the row. Integer index or string column name."
      ) do |title_column|
      options.title_column = title_column
    end
    opts.on("--text_column=COLNAME",
            "If target file is a CSV, which column contains the main, searchable of the row. Integer index or string column name."
      ) do |text_column|
      options.text_column = text_column
    end

    opts.on("-o", "--[no-]ocr", "don't attempt to OCR any PDFs, even if they contain no text") do |v|
      options.ocr = v 
    end

    opts.on( '-?', '--help', 'Display this screen' ) do     
      puts opts
      exit
    end
  end

  op.parse!

  # to delete an index: curl -X DELETE localhost:9200/indexname/
  unless ARGV.length == 1
    puts op
    exit
  end
end

# you can provide either a path to files locally or
# an S3 endpoint as s3://int-data-dumps/YOURINDEXNAME
FOLDER = ARGV.shift


ES_INDEX =  if options.index.nil? || options.index == ''
              if(FOLDER.downcase.include?('s3://'))
                s3_path_without_bucket = FOLDER.gsub(/s3:\/\//i, '').split("/", 2).last
                s3_path_without_bucket.gsub(/^.+\//, '').gsub(/[^A-Za-z0-9\-_]/, '')
              else
                FOLDER.gsub(/^.+\//, '').gsub(/[^A-Za-z0-9\-_]/, '')
              end
            else
              options.index
            end

S3_BUCKET = FOLDER.downcase.include?('s3://') ? FOLDER.gsub(/s3:\/\//i, '').split("/", 2).first : options.s3bucket
ES_HOST = options.host || "localhost:9200"
S3_PATH = options.s3path  || options.index
S3_BASEPATH = "https://#{S3_BUCKET}.s3.amazonaws.com/#{S3_PATH}"

raise ArgumentError, "specify a destination" unless FOLDER
raise ArgumentError, "specify the elasticsearch host" unless ES_HOST

###############################
# actual stuff
###############################

if __FILE__ == $0
  f = Stevedore::ESUploader.new(ES_HOST, ES_INDEX, S3_BUCKET, S3_BASEPATH)
  f.should_ocr = options.ocr
  puts "Will not OCR, per --no-ocr option" unless f.should_ocr

  if FOLDER.match(/\.[ct]sv$/)
    f.do_csv!(FOLDER, File.join(f.s3_basepath, File.basename(FOLDER)), options.title_column, options.text_column)
  else
    f.do!(FOLDER)
  end
  puts "Finished uploading documents at #{Time.now}"

  puts "Created Stevedore for #{ES_INDEX}; go check out https://stevedore.newsdev.net/search/#{ES_INDEX} or http://stevedore.adm.prd.newsdev.nytimes.com/search/#{ES_INDEX}"  
  if f.errors.size > 0  
    STDERR.puts "#{f.errors.size} failed documents:"
    STDERR.puts f.errors.inspect 
    puts "Uploading successful, but with #{f.errors.size} errors."
  end
end
