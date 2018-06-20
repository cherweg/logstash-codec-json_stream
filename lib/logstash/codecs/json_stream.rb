# encoding: utf-8
require "logstash/codecs/base"
require "logstash/util/charset"
require "logstash/json"


# This codec will decode streamed JSON that is not delimited.
# Encoding will emit a single JSON string ending in a `@delimiter`

class LogStash::Codecs::JSONStream < LogStash::Codecs::Base

  config_name "json_stream"

  config :charset, :validate => ::Encoding.name_list, :default => "UTF-8"

  public

  def register
    @converter = LogStash::Util::Charset.new(@charset)
    @converter.logger = @logger
  end

  def decode(concatenated_json, &block)
    decode_unsafe(concatenated_json, &block)
  rescue LogStash::Json::ParserError => e
    @logger.error("JSON parse error for json stream / concatenated json, original data now in message field", :error => e, :data => concatenated_json)
    yield LogStash::Event.new("message" => concatenated_json, "tags" => ["_jsonparsefailure"])
  rescue StandardError => e
    # This should NEVER happen. But hubris has been the cause of many pipeline breaking things
    # If something bad should happen we just don't want to crash logstash here.
    @logger.error(
      "An unexpected error occurred parsing JSON data",
      :data => concatenated_json,
      :message => e.message,
      :class => e.class.name,
      :backtrace => e.backtrace
    )
  end

  def encode(event)
    @logger.error("Encoding is not supported by 'concatenated_json' plugin yet")
  end

  def flush(&block)
    @logger.debug("empty flush method -- nothing to do")
  end

  private
  def decode_unsafe(concatenated_json)
    array_json = @converter.convert("[#{concatenated_json.gsub('}{', '},{')}]")
    LogStash::Json.load(array_json).each do |decoded_event|
       yield(LogStash::Event.new(decoded_event))
    end
  end
end
