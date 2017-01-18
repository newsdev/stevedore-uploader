require 'json'
require 'digest/sha1'

module Stevedore
  class StevedoreBlob
    attr_accessor :title, :text, :download_url, :extra
    def initialize(title, text, download_url=nil, extra={})
      self.title = title || download_url
      self.text = text
      self.download_url = download_url
      self.extra = extra
      raise ArgumentError, "StevedoreBlob extra support not yet implemented" if extra.keys.size > 0
    end

    def clean_text
      @clean_text ||= text.gsub(/<\/?[^>]+>/, '') # removes all tags
    end 

    def self.new_from_tika(content, metadata, download_url, filename)
      self.new( ((metadata["title"] && metadata["title"] != "Untitled") ? metadata["title"] : File.basename(filename)), content, download_url)
    end

    def analyze!
      # probably does nothing on blobs.
      # this should do the HTML boilerplate extraction thingy on HTML.
    end

    def to_hash
      {
        "sha1" => Digest::SHA1.hexdigest(download_url),        
        "title" => title.to_s,
        "source_url" => download_url.to_s,
        "file" => {
          "title" => title.to_s,
          "file" => clean_text.to_s
        },
        "analyzed" => {
          "body" => clean_text.to_s,
          "metadata" => {
            "Content-Type" => extra["Content-Type"] || "text/plain"
          }
        },
        "_updatedAt" => Time.now      
      }
    end

    # N.B. the elasticsearch gem converts your hashes to JSON for you. You don't have to use this at all.
    # def to_json
    #   JSON.dump to_hash
    # end
  end
end
