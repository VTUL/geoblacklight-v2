require 'csv'
require 'digest'
require 'fileutils'
require 'json'
require 'net/http'
require 'uri'
require 'yaml'

class DataIngest

  @@fields = {
    "dc_identifier_s": {required: true},
    "dc_rights_s": {required: true},
    "dct_provenance_s": {required: true},
    "dct_references_s": {required: false},
    "dc_creator_sm": {required: false},
    "dc_language_sm": {required: false},
    "dc_publisher_sm": {required: false},
    "dc_type_s": {required: false},
    "dct_spatial_sm": {required: false},
    "dct_temporal_sm": {required: false},
    "dct_issued_dt": {required: false},
    "dct_ispartof_sm": {required: false},
    "solr_geom": {required: true},
    "georss:polygon": {required: false},
    "dc_title_s": {required: true},
    "dc_description_s": {required: false},
    "dc_format_s": {required: false},
    "dc_subject_sm": {required: false},
    "layer_id_s": {required: false},
    "layer_modified_dt": {required: false},
    "layer_slug_s": {required: true},
    "layer_geom_type_s": {required: false},
    "geoblacklight_version": {required: true}
  }
    
  def createreport(filename, content)
    File.open(filename, 'w') { |file| file.write(content) }
  end


  def getfilelist(folderpath)
    return Dir[folderpath].select{ |filename| File.file? filename }.map{ |filename| File.basename filename }
  end


  def getfileprefix(filename)
    if filename.start_with?("dmf_")
      return "dmf_"
    elsif filename.start_with?("cgit_")
      return "cgit_"
    elsif filename.start_with?("gdrive_")
      return "gdrive_"
    else
      return ""
    end
  end

  def formatsolrdata(content)
    timestring = Time.now().strftime("%Y-%m-%dT%H:%M:%SZ") 
    content = content.merge(content) { |k, v1, v2| sanitize_string v2 }

    if !content['dct_references_s'].nil?
      content['dct_references_s'] = "{\"http://schema.org/downloadUrl\":\"" + content['dc_identifier_s'] + "\",\"http://www.opengis.net/def/serviceType/ogc/wcs\":\"" + content['dct_references_s'] + "\"}"
    end
    gdata = content['solr_geom'].split(",")
    content['solr_geom'] = "ENVELOPE("+ gdata[1] + "," + gdata[3] + "," + gdata[2] + "," + gdata[0] + ")"

    content['layer_modified_dt'] = timestring

    return content
  end

  def is_number? string
    true if Float(string) rescue false
  end


  def is_valid_url(urlstring)
    begin
      uri = URI.parse(urlstring)
      if uri.kind_of?(URI::HTTP) or uri.kind_of?(URI::HTTPS)
        return true
      else
        return false
      end
    rescue
      return false
    end
  end


  def validaterecord(row)
    if row.length === 0
      return "Row does not contain record."
    end

    result = ""
    row.each do |key, content|
      begin
        if @@fields.key?(key.to_sym) && @@fields[key.to_sym][:required] && content.blank?
          result += "#{key} is required but empty. "

        elsif key == "dc_identifier_s" && !is_valid_url(content)
          result += "dc_identifier_s field is not a valid URL. "
      
        elsif key == "solr_geom" && (!content.respond_to?("split") || content.split(",").length != 4)
          result += "solr_geom field is incorrect. "

        elsif key == "solr_geom" && (!content.respond_to?("split") || !content.split(",").all? {|i| is_number?( i ) })
          result += "solr_geom field should be all numbers. "
        end
      rescue
        Rails.logger.error("Unknown error parsing row")
      end
    end
    return result
  end

  def sanitize_string(content)
    if !content.nil?
      encoding_options = {
        :invalid           => :replace,
        :undef             => :replace,
        :replace           => ' ',
        :universal_newline => true
      }
      result = content.encode(Encoding.find('ASCII'), encoding_options).squish
    else
      result = nil
    end
    return result
  end


  def dirMissing dir
    marker = "===================================\n"
    error = "#{dir} does not exist. Halting."
    message = marker + Time.now.inspect + " - " + error + "\n" + marker
    Rails.logger.error message
    raise IOError
  end

  def run
    basepath = Rails.application.secrets.ingest_dir || "/opt/sftp/geodata"
    archivepath = File.join(basepath, "Archive")
    reportpath = File.join(basepath, "Report")
    errorpath = File.join(reportpath, "Errors")
    logpath = File.join(reportpath, "Logs")
    uploadpath = File.join(basepath, "Upload")

    dirMissing(uploadpath) unless Dir.exists?(uploadpath)
    
    list_of_files = getfilelist(File.join(uploadpath, "*"))
    for uploadfile in list_of_files
      puts "Processing #{uploadfile}"
      prefix = getfileprefix(uploadfile)
      if prefix.length > 0
        
        errorcontent = ""
        totalrecs = 0
        ingestedrecs = 0
        index = 1

        file_array = []
        csv = nil
        file_error = false
        begin
          csv = CSV::parse(File.open(File.join(uploadpath, uploadfile), "r:UTF-8", &:read))
        rescue ArgumentError => e
          errorcontent += "#{uploadfile} could not be read. #{e}"            
          file_error = true
        end
        if csv
          fields = csv.shift.map { |f| f.downcase.gsub(" ", "_")}
          csv_hash = csv.collect { |record| Hash[*fields.zip(record).flatten] }
          csv_hash.each do |row|
            puts "Processing row #{index.to_s}"
            errmsg = validaterecord(row)
            if errmsg.length > 0
              errorcontent += "row " + (index + 1).to_s + ": " + errmsg + "\n\n"
            else
              solrdata = formatsolrdata(row)
              begin
                Blacklight.default_index.connection.add(solrdata)
                Blacklight.default_index.connection.commit
                ingestedrecs += 1
              rescue StandardError => e 
		errorcontent += "row #{(index + 1).to_s}: There was an error committing this record to solr. Message: #{e.message}\n\n"
              end
            end

            totalrecs += 1
            index += 1
          end
        end        

        # create log report file
        logfilename = File.join(logpath, uploadfile + "_" + Time.now().strftime("%Y%m%d%H%M%S").to_s + ".log.txt")
        logcontent = uploadfile + ": Total ingest records: " + totalrecs.to_s + ", ingested " + ingestedrecs.to_s + " records."
        createreport(logfilename, logcontent)

        # create error report file
        if totalrecs != ingestedrecs || file_error
          errorfilename = File.join(errorpath, uploadfile + "_" + Time.now().strftime("%Y%m%d%H%M%S").to_s + ".error.txt")
          createreport(errorfilename, uploadfile + "\n" + errorcontent)
        end

        # move upload file to archive folder
        src = File.join(uploadpath, uploadfile)
        dest = File.join(archivepath, uploadfile)
        FileUtils.mv(src, dest)
      
      end
    end
  end

end
