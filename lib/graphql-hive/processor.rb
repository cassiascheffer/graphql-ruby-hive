# frozen_string_literal: true

require "graphql-hive/report"

module GraphQLHive
  class Processor
    def initialize(queue:, logger:, sampler:, client:, buffer_size:, client_info: nil)
      @queue = queue
      @logger = logger
      @sampler = sampler
      @client_info = client_info
      @client = client
      @buffer_size = buffer_size
      @buffer = []
    end

    def process_queue
      while process_next_operation
        flush_buffer if buffer_full?
      end

      flush_buffer unless @buffer.empty?
    end

    private

    def process_next_operation
      operation = @queue.pop
      return false unless operation

      @buffer << operation if @sampler.sample?(operation)
      true
    rescue ClosedQueueError
      false
    rescue => e
      @logger.error("Failed to process operation: #{e.message}")
      true # Continue processing
    end

    def buffer_full?
      @buffer.size >= @buffer_size
    end

    def flush_buffer
      return if @buffer.empty?

      @logger.debug("Flushing #{@buffer.size} operations")
      send_report
      @buffer.clear
    rescue => e
      @logger.error("Failed to flush buffer: #{e.message}")
      @buffer.clear
    end

    def send_report
      report = Report.new(operations: @buffer, client_info: @client_info).build
      @client.send(:"/usage", report, :usage)
    end
  end
end
