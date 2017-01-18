require 'digest/sha1'

module Stevedore
  class StevedoreCsvRow < StevedoreBlob
    attr_accessor :title, :text, :download_url, :whole_row, :row_num
    def initialize(title, text, row_num, download_url, whole_row={})
      self.title = title || download_url
      self.text = text
      self.download_url = download_url
      self.whole_row = whole_row
      self.row_num = row_num
    end

    def clean_text
      @clean_text ||= text.gsub(/<\/?[^>]+>/, '') # removes all tags
    end 

    def to_hash
      {
        "sha1" => Digest::SHA1.hexdigest(download_url + row_num.to_s),        
        "title" => title.to_s,
        "source_url" => download_url.to_s,
        "file" => {
          "title" => title.to_s,
          "file" => clean_text.to_s
        },
        "analyzed" => {
          "body" => clean_text.to_s,
          "metadata" => {
            "Content-Type" => "text/plain"
          }.merge(  whole_row.to_h  )
        },
        "_updatedAt" => DateTime.now      
      }
    end
  end
end
