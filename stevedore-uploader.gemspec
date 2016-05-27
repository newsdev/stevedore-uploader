Gem::Specification.new do |s|
  s.name        = 'stevedore-uploader'
  s.version     = '1.0.2'
  s.licenses    = ['MIT']
  s.summary     = "Upload documents to a Stevedore search engine."
  s.description = "TK"
  s.platform    = "java"
  s.authors     = ["Jeremy B. Merrill"]
  s.email       = 'jeremy.merrill@nytimes.com'
  s.files       = ["bin/upload_to_elasticsearch.rb", "README.md"] +  Dir['lib/**/*']
  s.homepage    = 'https://github.com/newsdev/stevedore-uploader'
  s.add_dependency("elasticsearch", "~> 1.0")
  s.add_dependency("manticore")
  s.add_dependency("jruby-openssl", "~> 0.9")  
  s.add_dependency("aws-sdk", "~> 2")  
  s.add_dependency("rika-stevedore", ">= 1.6.1")  
  s.add_dependency("nokogiri", "~> 1.6")
  s.add_dependency("pst", "~> 0.0.2")
  s.add_dependency("mail", "~> 2.6")
  s.add_dependency("rubyzip", "~> 1.1")

end
