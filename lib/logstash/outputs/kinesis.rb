# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"


class LogStash::Outputs::Kinesis < LogStash::Outputs::Base
  config_name "kinesis"
  milestone 1

  # Kinesis stream to write to. This can be dynamic using the %{foo} syntax.
  config :stream, :validate => :string, :required => true

  # The credentials of the AWS account used to access the bucket.
  # Credentials can be specified:
  # - As an ["id","secret"] array
  # - As a path to a file containing AWS_ACCESS_KEY_ID=... and AWS_SECRET_ACCESS_KEY=...
  # - In the environment (variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)
  config :credentials, :validate => :array, :default => nil

  # The partition key is used as part of the hash to determine which shard data is put on. This can be dynamic using the %{foo} syntax.
  config :partition_key, :validate => :string, :required => true

  public
  def register
    require "aws-sdk"
    require "base64"

    @logger.info("Registering kinesis output", :stream => @stream, :partition_key => @partition_key)

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

    @kinesis = AWS::Kinesis.new(
      :access_key_id => @access_key_id,
      :secret_access_key => @secret_access_key
    ).client
  end

  public
  def receive(event)
    return unless output?(event)

    if event == LogStash::SHUTDOWN
      finished
      return
    end

    stream = event.sprintf(@stream)
    partition_key = event.sprintf(@partition_key)

    resp = @kinesis.put_record(
      :stream_name => stream,
      :data => Base64.encode64(event.to_json),
      :partition_key => partition_key
    )
    @logger.debug("put event to kinesis", :shard_id => resp[:shard_id], :sequence_number => resp[:sequence_number])
  end
end
