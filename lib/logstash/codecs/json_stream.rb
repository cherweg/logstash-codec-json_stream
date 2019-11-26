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
    array_json = @converter.convert("[#{concatenated_json.gsub('}{', '},{')}]")
    parse(array_json, &block)
  rescue LogStash::Json::ParserError => e
    @logger.warn("JSON parse error for json stream / concatenated json, original data now in message field", :error => e, :data => concatenated_json)
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
    @logger.error("Encoding is not supported by 'jsonstream' plugin yet")
  end

  private

  # from_json_parse uses the Event#from_json method to deserialize and directly produce events
  def from_json_parse(json, &block)
    LogStash::Event.from_json(json).each { |event| yield event }
  rescue LogStash::Json::ParserError => e
    @logger.warn("JSON parse error, original data now in message field", :error => e, :data => json)
    yield LogStash::Event.new("message" => json, "tags" => ["_jsonparsefailure"])
  end

  # legacy_parse uses the LogStash::Json class to deserialize json
  def legacy_parse(json, &block)
    # ignore empty/blank lines which LogStash::Json#load returns as nil
    o = LogStash::Json.load(json)
    yield(LogStash::Event.new(o)) if o
  rescue LogStash::Json::ParserError => e
    @logger.warn("JSON parse error, original data now in message field", :error => e, :data => json)
    yield LogStash::Event.new("message" => json, "tags" => ["_jsonparsefailure"])
  end

  # keep compatibility with all v2.x distributions. only in 2.3 will the Event#from_json method be introduced
  # and we need to keep compatibility for all v2 releases.
  alias_method :parse, LogStash::Event.respond_to?(:from_json) ? :from_json_parse : :legacy_parse
end
