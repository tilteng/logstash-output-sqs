# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require 'logstash/outputs/sqs'
require "aws-sdk"
require "json"

describe LogStash::Outputs::SQS do
  let(:event) { LogStash::Event.new(:msg => "msg") }
  let(:large_event) { LogStash::Event.new(:msg => "1" * 100) }
  let(:mock_sqs) { Aws::SQS::Client.new({ :stub_responses => true }) }

  subject { LogStash::Outputs::SQS.new(config) }

  before(:each) do
    expect(Aws::SQS::Client).to receive(:new).and_return(mock_sqs)
    expect(mock_sqs).to receive(:get_queue_url).and_return({ queue_url: "https://testqueue" })
    subject.register
  end

  context "deprecated configuration" do
    let(:config) do
      {
        "queue" => "testqueue",
        "batch_events" => 3
      }
    end

    it "sends message after batch size hit (:batch_events configuration)" do
      expect(mock_sqs).to receive(:send_message).once

      subject.receive(event)
      subject.receive(event)
      subject.receive(event)
    end
  end

  context "valid configuration" do
    let(:config) do
      {
        "queue" => "testqueue",
        "batch_size" => 2,
        "batch_timeout" => 2,
        "batch_bytesize" => 100,
      }
    end

    it "sends no messages when no limits are hit" do
      expect(mock_sqs).not_to receive(:send_message)

      subject.receive(event)
    end

    it "sends message when batch size is hit" do
      expect(mock_sqs).to receive(:send_message) do |msg|
        expect(msg[:queue_url]).to eq("https://testqueue")
        events = JSON.parse(msg[:message_body])
        expect(events.length).to eq(2)
        expect(events[0]).to include("msg" => "msg")
        expect(events[1]).to include("msg" => "msg")
      end

      subject.receive(event)
      subject.receive(event)
    end

    it "sends message when time limit is hit" do
      expect(mock_sqs).to receive(:send_message).once

      subject.receive(event)
      sleep 3
    end

    it "sends message when byte size is hit" do
      expect(mock_sqs).to receive(:send_message).once

      subject.receive(large_event)
    end
  end
end
