# encoding: utf-8
require "logstash/codecs/base"
require "logstash/util/charset"
require "logstash/util/buftok"
require "logstash/json"

# This codec will decode streamed JSON that is not delimited.
# Encoding will emit a single JSON string ending in a `@delimiter`

class LogStash::Codecs::JSONStream < LogStash::Codecs::Base
  config_name "json_stream"

  config :charset, :validate => ::Encoding.name_list, :default => "UTF-8"

  # Change the delimiter that separates lines
  config :delimiter, :validate => :string, :default => "\n"

  public

  def register
    @converter = LogStash::Util::Charset.new(@charset)
    @converter.logger = @logger
  end

  def decode(data, &block)
    io = StringIO.new data

    loop.inject(counter: 0, string: '') do |acc|
      char = io.getc

      break if char.nil? # EOF
      next acc if acc[:counter].zero? && char != '{' # between objects

      acc[:string] << char

      if char == '}' && (acc[:counter] -= 1).zero?
        # ⇓⇓⇓ # CALLBACK, feel free to JSON.parse here
        parse(@converter.convert(acc[:string].gsub(/\p{Space}+/, ' ')), &block)
        next {counter: 0, string: ''} # from scratch
      end

      acc.tap do |result|
        result[:counter] += 1 if char == '{'
      end
    end
  end

  def encode(event)
    # Tack on a @delimiter for now because previously most of logstash's JSON
    # outputs emitted one per line, and whitespace is OK in json.
    @on_event.call(event, "#{event.to_json}#{@delimiter}")
  end

  def flush(&block)
    remainder = @buffer.flush
    if !remainder.empty?
      parse(@converter.convert(remainder), &block)
    end
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
