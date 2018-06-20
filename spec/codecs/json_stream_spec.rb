# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/json_stream"
require "logstash/event"
require "logstash/json"
require "insist"

describe LogStash::Codecs::JSONStream do

  class LogStash::Codecs::JSONStream
    public :decode_unsafe # use method without error logging for better visibility of errors
  end

  let(:codec_options) { {} }

  context "default parser choice" do
    subject do
      LogStash::Codecs::JSONStream.new(codec_options)
    end

    it "should read multiple events" do
      events = events_from_string(<<-EOS
        {"messageType": "CONTROL_MESSAGE", "message": "foo"}
        {"messageType": "DATA_MESSAGE", "logGroup": "testing", "logEvents": [
          {"id": "4711", "@timestamp": "2018-06-18T13:36:25.484+00:00", "message": "{\\"tasks\\": \\"READING\\"}"},
          {"id": "1848", "@timestamp": "1989-11-09T23:59:25.484+02:00", "message": "{\\"tasks\\": \\"WRITING\\"}"}
        ]}
EOS
      )
      insist { events.size } == 2

      control_event = events[0]
      data_event = events[1]

      insist { control_event.is_a? LogStash::Event }
      insist { control_event.get("messageType") } == "CONTROL_MESSAGE"
      insist { control_event.get("message") } == "foo"

      insist { data_event.is_a? LogStash::Event }
      insist { data_event.get("messageType") } == "DATA_MESSAGE"
      insist { data_event.get("logGroup") } == "testing"
      insist { data_event.get("logEvents").size } == 2

      insist { data_event.get("logEvents")[0]['id'] } == '4711'
      insist { data_event.get("logEvents")[0]['@timestamp'] } == '2018-06-18T13:36:25.484+00:00'
      insist { data_event.get("logEvents")[0]['message'] } == '{"tasks": "READING"}'

      insist { data_event.get("logEvents")[1]['id'] } == '1848'
      insist { data_event.get("logEvents")[1]['@timestamp'] } == '1989-11-09T23:59:25.484+02:00'
      insist { data_event.get("logEvents")[1]['message'] } == '{"tasks": "WRITING"}'
    end

    it "should read multiple events from data" do
      events = events_from_file('log-stream.valid-line-formatted')
      insist { events.size } == 5

      events.each do |event|
        insist { event.is_a? LogStash::Event }
        insist { event.get("logGroup") } == "test-core"
        insist { event.get("messageType") } == "DATA_MESSAGE"
        insist { event.get("logEvents").size } != 0
        event.get("logEvents").each do |event|
          insist { event["id"] } != nil
          insist { event["message"] } != nil
        end
      end
    end

    it "should not fail with stacktrace" do
      events = events_from_file('log-stream.minimal-failure-formatted')
      insist { events.size } == 1

      insist { events[0].is_a? LogStash::Event }
      insist { events[0].get("logEvents").size } == 1
      insist { events[0].get("logEvents")[0]['message']  =~ /Failed at: $\{springMacroRequestContext.getMessag\.\.\./ }
    end
  end

  private
  def events_from_file fixture_logfile_name
    data = IO.read(File.join(File.dirname(__FILE__), "../../fixtures/#{fixture_logfile_name}"))
    events_from_string data
  end

  def events_from_string data
    events = []
    data_without_formatting = data.gsub(/(\n|\s{2,})/, '')
    subject.decode_unsafe(data_without_formatting) { |event| events << event }
    events
  end
end
