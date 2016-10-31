# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "stud/buffer"

# Push events to an Amazon Web Services Simple Queue Service (SQS) queue.
#
# SQS is a simple, scalable queue system that is part of the 
# Amazon Web Services suite of tools.
#
# Although SQS is similar to other queuing systems like AMQP, it
# uses a custom API and requires that you have an AWS account.
# See http://aws.amazon.com/sqs/ for more details on how SQS works,
# what the pricing schedule looks like and how to setup a queue.
#
# To use this plugin, you *must*:
#
#  * Have an AWS account
#  * Setup an SQS queue
#  * Create an identify that has access to publish messages to the queue.
#
# The "consumer" identity must have the following permissions on the queue:
#
#  * sqs:ChangeMessageVisibility
#  * sqs:ChangeMessageVisibilityBatch
#  * sqs:GetQueueAttributes
#  * sqs:GetQueueUrl
#  * sqs:ListQueues
#  * sqs:SendMessage
#  * sqs:SendMessageBatch
#
# Typically, you should setup an IAM policy, create a user and apply the IAM policy to the user.
# A sample policy is as follows:
# [source,ruby]
#      {
#        "Statement": [
#          {
#            "Sid": "Stmt1347986764948",
#            "Action": [
#              "sqs:ChangeMessageVisibility",
#              "sqs:ChangeMessageVisibilityBatch",
#              "sqs:GetQueueAttributes",
#              "sqs:GetQueueUrl",
#              "sqs:ListQueues",
#              "sqs:SendMessage",
#              "sqs:SendMessageBatch"
#            ],
#            "Effect": "Allow",
#            "Resource": [
#              "arn:aws:sqs:us-east-1:200850199751:Logstash"
#            ]
#          }
#        ]
#      }
#
# See http://aws.amazon.com/iam/ for more details on setting up AWS identities.
#
class LogStash::Outputs::SQS < LogStash::Outputs::Base
  include LogStash::PluginMixins::AwsConfig::V2
  include Stud::Buffer

  config_name "sqs"

  # Name of SQS queue to push messages into. Note that this is just the name of the queue, not the URL or ARN.
  config :queue, :validate => :string, :required => true

  # Maximum number of events in a batch.
  config :batch_size, :validate => :number, :default => 10

  # Maximum time between sending messages to SQS in a batch.
  config :batch_timeout, :validate => :number, :default => 10

  # Maximum total bytesize of a batch.
  config :batch_bytesize, :validate => :number, :default => 60 * 1024

  # Deprecated configuration options
  config :batch, :validate => :boolean, :default => false
  config :batch_events, :validate => :number, :default => 0

  public
  def register
    require "aws-sdk"

    if @batch
      @logger.warn(":batch configuration option is now deprecated. Set :batch_size to 1 to immediately flush each event to SQS.")
    end
    if @batch_events != 0
      @logger.warn(":batch_events configuration option is now deprecated. Use :batch_size instead.")
      @batch_size = @batch_events
    end

    @aws_sqs_client = Aws::SQS::Client.new(aws_options_hash)
    @queue_url = @aws_sqs_client.get_queue_url(:queue_name => @queue)[:queue_url]

    @current_bytesize = 0
    @current_bytesize_mutex = Mutex.new

    buffer_initialize(
      :max_items => @batch_size,
      :max_interval => @batch_timeout,
      :logger => @logger
    )
  end # def register

  def receive(event)
    data = event.to_json
    buffer_receive(data)
    increment_bytesize(data)
  end # def receive

  # called from Stud::Buffer#buffer_flush when there are events to flush
  def flush(events, close=false)
    decrement_bytesize(events)
    @aws_sqs_client.send_message({
      queue_url: @queue_url,
      message_body: "[#{events.join(",")}]"
    })

  end

  def on_flush_error(e)
    @logger.error(
      "Error flushing to SQS",
      :exception => e.class.name,
      :backtrace => e.backtrace
    )
  end

  def close
    buffer_flush(:final => true)
  end

  private
  def decrement_bytesize(items)
    total_size = items.reduce(0) { |prev, curr| prev + curr.bytesize }
    bytesize = @current_bytesize_mutex.synchronize { @current_bytesize -= total_size }
  end

  private
  def increment_bytesize(item)
    bytesize = @current_bytesize_mutex.synchronize { @current_bytesize += item.bytesize }
    if bytesize > @batch_bytesize
      buffer_flush(:force => true)
    end
  end
end
