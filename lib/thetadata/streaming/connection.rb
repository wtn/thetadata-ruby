require "async"

module ThetaData
  module Streaming
    class Connection
      def initialize(stream)
        @stream = stream
        @framer = Protocol::FPSS::Framer.new(stream)
      end

      def closed?
        @stream.closed?
      end

      def close
        @framer.close unless closed?
      end

      def write_frame(type, payload = nil)
        @framer.write(type, payload)
      end

      def read_frame
        @framer.read_frame
      rescue EOFError
        nil
      end

      def read_frame_with_timeout(timeout)
        Async::Task.current.with_timeout(timeout) do
          read_frame
        end
      rescue Async::TimeoutError
        raise ThetaData::TimeoutError, "Read timed out after #{timeout}s"
      end

      def flush
        @framer.flush
      end

      def each_frame
        return enum_for(:each_frame) unless block_given?

        until closed?
          frame = read_frame
          break if frame.nil?
          yield frame
        end
      end
    end
  end
end
