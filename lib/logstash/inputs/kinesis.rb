# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"


# Stream events from Amazon's Kinesis

class LogStash::Inputs::Kinesis < LogStash::Inputs::Base
  config_name "kinesis"
  milestone 1

  default :codec, "plain"

  # Kinesis stream to read from
  config :stream, :validate => :string, :required => true

  # Shards to read from, leave empty for all
  config :shards, :validate => :array, :default => []

  # The credentials of the AWS account used to access the bucket.
  # Credentials can be specified:
  # - As an ["id","secret"] array
  # - As a path to a file containing AWS_ACCESS_KEY_ID=... and AWS_SECRET_ACCESS_KEY=...
  # - In the environment (variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)
  config :credentials, :validate => :array, :default => nil

  # Number of records to get at a time
  config :batch, :validate => :number, :default => 500

  # Time to wait before attempting to get more records after getting to the end of the stream
  config :sleep, :validate => :number, :default => 0.25

  public
  def register
    require "kinesis-client"

    @logger.info("Registering kinesis input", :stream => @stream)

    # Lifted from the s3 input
    if @credentials.nil?
      @access_key_id = ENV['AWS_ACCESS_KEY_ID']
      @secret_access_key = ENV['AWS_SECRET_ACCESS_KEY']
    elsif @credentials.is_a? Array
      if @credentials.length ==1
        File.open(@credentials[0]) { |f| f.each do |line|
          unless (/^\#/.match(line))
            if(/\s*=\s*/.match(line))
              param, value = line.split('=', 2)
              param = param.chomp().strip()
              value = value.chomp().strip()
              if param.eql?('AWS_ACCESS_KEY_ID')
                @access_key_id = value
              elsif param.eql?('AWS_SECRET_ACCESS_KEY')
                @secret_access_key = value
              end
            end
          end
        end
        }
      elsif @credentials.length == 2
        @access_key_id = @credentials[0]
        @secret_access_key = @credentials[1]
      else
        raise ArgumentError.new('Credentials must be of the form "/path/to/file" or ["id", "secret"]')
      end
    end
    if @access_key_id.nil? or @secret_access_key.nil?
      raise ArgumentError.new('Missing AWS credentials')
    end

    shard_ids = if @shards.empty?
      :all
    else
      @shards
    end
    AWS.config(
      access_key_id: @access_key_id,
      secret_access_key: @secret_access_key
    )
    @client = Kinesis::Client::Stream.new(@stream,
                                          shard_ids: shard_ids,
                                          batch_size: @batch,
                                          sleep_seconds: @sleep
              )
  end # def register

  public
  def run(queue)
    @client.run do |message|
      decorate(message)
      queue << message
    end
    finished
  end # def run
end
