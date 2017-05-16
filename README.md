stevedore-uploader
==================

A tool for uploading documents into [Stevedore](https://github.com/newsdev/stevedore), a flexible, extensible search engine for document dumps created by The New York Times.

Stevedore is essentially an ElasticSearch endpoint with a customizable frontend attached to it. Stevedore's primary document store is ElasticSearch, so `stevedore-uploader`'s primary task is merely uploading documents to ElasticSearch, with a few attributes that Stevedore depends on. Getting a new document set ready for search requires a few steps, but this tool helps with the hardest one: Converting the documents you want to search into a format that ElasticSearch understands. Customizing the search interface is often not necessary, but if it is, information on how to do that is in the [Stevedore](https://github.com/newsdev/stevedore) repository.

Every document processing job is different. Some might require OCR, others might require parsing e-mails, still others might call for sophisticated processing of text documents. There's no telling. That being the case, this project tries to make no assumptions about the type of data you'll be uploading -- but by default tries to convert everything into plaintext with [Apache Tika](https://tika.apache.org/). Stevedore distinguishes between a few default types, like emails and text blobs, (and PRs would be appreciated adding new ones); for specialized types, the `do` function takes a block allowing you to modify the documents with just a few lines of Ruby.

For more details on the entire workflow, see [Stevedore](https://github.com/newsdev/stevedore)

Installation
------------

This project is in JRuby, so we can leverage the transformative enterprise stability features of the JVM Java TM Platform and the truly-American Bruce-Springsteen-Born-to-Run freedom-to-roam of Ruby. (And the fact that Tika's in Java.)

1. install jruby. if you use rbenv, you'd do this:
`rbenv install jruby-9.0.5.0` (or greater versions okay too)
2. be sure you're running Java 8. (java 7 is deprecated, c'mon c'mon)
3. `bundle install`

Command-Line Options
--------------------
````
Usage: stevedore [options] target_(dir_or_csv)
    -h, --host=SERVER:PORT           The location of the ElasticSearch server
    -i, --index=NAME                 A name to use for the ES index (defaults to using the directory name)
    -s, --s3path=PATH                The path under your bucket where these files have been uploaded. (defaults to ES index)
    -b, --s3bucket=PATH              The s3 bucket where these files have already been be uploaded (or will be later).
        --title_column=COLNAME       If target file is a CSV, which column contains the title of the row. Integer index or string column name.
        --text_column=COLNAME        If target file is a CSV, which column contains the main, searchable of the row. Integer index or string column name.
    -o, --[no-]ocr                   don't attempt to OCR any PDFs, even if they contain no text
    -?, --help                       Display this screen
````


Advanced Usage
--------------

**This is a piece of a larger upload workflow, [described here](https://github.com/newsdev/stevedore/blob/master/README.md). You should read that first, then come back here.**

upload documents from your local disk
```
bundle exec ruby bin/stevedore.rb --index=INDEXNAMEx [--host=localhost:9200]  [--s3path=name-of-path-under-bucket] path/to/documents/to/parse
```
or from s3
```
bundle exec ruby bin/stevedore.rb --index=INDEXNAMEx [--host=localhost:9200]   s3://my-bucket/path/to/documents/to/parse
```

if host isn't specified, we assume `localhost:9200`.

e.g. 
```
bundle exec ruby bin/stevedore.rb --index=jrubytest --host=https://stevedore.elasticsearch.yourdomain.net/es/ ~/code/marco-rubios-emails/emls/ 
```

you may also specify an s3:// location of documents to parse, instead of a local directory, e.g.
```
bundle exec ruby bin/stevedore.rb --index=jrubytest --host=https://stevedore.elasticsearch.yourdomain.net/es/ s3://int-data-dumps/marco-rubio-fire-drill
```
if you choose to process documents from S3, you should upload those documents using your choice of tool -- but `awscli` is a good choice. *Stevedore-Uploader does NOT upload documents to S3 on your behalf.

If you need to process documents in a specialized, customized way, follow this example:
````
uploader = Stevedore::ESUploader.new(ES_HOST, ES_INDEX, S3_BUCKET, S3_PATH_PREFIX) # S3_BUCKET, S3_PATH_PREFIX are optional
uploader.do! FOLDER do |doc, filename, content, metadata|
  next if doc.nil?
  doc["analyzed"]["metadata"]["date"] = Date.parse(File.basename(filename).split("_")[-2])
  doc["analyzed"]["metadata"]["title"] = my_title_getter_function(File.basename(filename))
end
````

Questions?
==========

Hit us up in the [Stevedore](https://github.com/newsdev/stevedore) issues.