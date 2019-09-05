require 'geoblacklight/dataingest'

namespace :vtul do 
  namespace :geoblacklight do
    desc 'Ingest data into Solr from uploaded csv.'
    task data_ingest: :environment do
      ingestTools = DataIngest.new
      ingestTools.run

    end
  end
end