require "thetadata"
require "stringio"
require "async"

describe ThetaData::Streaming::Connection do
  with "initialization" do
    it "creates framer from stream" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)

      expect(connection).to be_a(ThetaData::Streaming::Connection)
    end
  end

  with "#closed?" do
    it "returns false for open stream" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)

      expect(connection.closed?).to be == false
    end

    it "returns true after closing" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)

      connection.close

      expect(connection.closed?).to be == true
    end
  end

  with "#close" do
    it "closes the stream" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)

      connection.close

      expect(stream.closed?).to be == true
    end

    it "is idempotent" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)

      connection.close
      connection.close

      expect(stream.closed?).to be == true
    end
  end

  with "#write_frame" do
    it "writes frame to stream" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)

      connection.write_frame(Protocol::FPSS::MessageType::PING, "\x00")
      connection.flush

      stream.rewind
      frame = Protocol::FPSS::Frame.read(stream)

      expect(frame.type).to be == Protocol::FPSS::MessageType::PING
      expect(frame.payload).to be == "\x00"
    end

    it "writes frame with nil payload" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)

      connection.write_frame(Protocol::FPSS::MessageType::PING, nil)
      connection.flush

      stream.rewind
      frame = Protocol::FPSS::Frame.read(stream)

      expect(frame.type).to be == Protocol::FPSS::MessageType::PING
    end
  end

  with "#read_frame" do
    it "reads frame from stream" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      frame = connection.read_frame

      expect(frame.type).to be == Protocol::FPSS::MessageType::PING
      expect(frame.payload).to be == "\x00"
    end

    it "returns nil on EOF" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)

      frame = connection.read_frame

      expect(frame).to be == nil
    end

    it "reads multiple frames sequentially" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "1").write(stream)
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "2").write(stream)
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "3").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)

      expect(connection.read_frame.payload).to be == "1"
      expect(connection.read_frame.payload).to be == "2"
      expect(connection.read_frame.payload).to be == "3"
      expect(connection.read_frame).to be == nil
    end
  end

  with "#flush" do
    it "flushes buffered writes" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)

      connection.write_frame(Protocol::FPSS::MessageType::PING, "\x00")
      connection.flush

      expect(stream.size).to be > 0
    end
  end

  with "#each_frame" do
    it "yields each frame until EOF" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "a").write(stream)
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "b").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      payloads = []

      connection.each_frame { |f| payloads << f.payload }

      expect(payloads).to be == ["a", "b"]
    end

    it "returns enumerator without block" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)

      enum = connection.each_frame

      expect(enum).to be_a(Enumerator)
    end

    it "handles empty stream" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)

      frames = connection.each_frame.to_a

      expect(frames).to be == []
    end

    it "can be chained with enumerator methods" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "a").write(stream)
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "b").write(stream)
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "c").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      payloads = connection.each_frame.map(&:payload).take(2)

      expect(payloads).to be == ["a", "b"]
    end
  end

  with "different frame types" do
    it "handles trade frames" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE, "trade_data").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      frame = connection.read_frame

      expect(frame.type).to be == Protocol::FPSS::MessageType::TRADE
      expect(frame.payload).to be == "trade_data"
    end

    it "handles quote frames" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::QUOTE, "quote_data").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      frame = connection.read_frame

      expect(frame.type).to be == Protocol::FPSS::MessageType::QUOTE
      expect(frame.payload).to be == "quote_data"
    end

    it "handles error frames" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::ERROR, "error message").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      frame = connection.read_frame

      expect(frame.type).to be == Protocol::FPSS::MessageType::ERROR
      expect(frame.payload).to be == "error message"
    end

    it "handles metadata frames" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::METADATA, "permissions").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      frame = connection.read_frame

      expect(frame.type).to be == Protocol::FPSS::MessageType::METADATA
      expect(frame.payload).to be == "permissions"
    end
  end

  with "binary data" do
    it "handles binary payloads" do
      stream = StringIO.new
      binary_data = "\x00\x01\x02\xFF\xFE\xFD"
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::TRADE, binary_data).write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      frame = connection.read_frame

      expect(frame.payload.bytes).to be == [0, 1, 2, 255, 254, 253]
    end

    it "handles empty payload" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::START, "").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      frame = connection.read_frame

      expect(frame.type).to be == Protocol::FPSS::MessageType::START
      expect(frame.payload).to be == nil
    end

    it "handles max size payload" do
      stream = StringIO.new
      max_data = "x" * 255
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::METADATA, max_data).write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)
      frame = connection.read_frame

      expect(frame.payload.length).to be == 255
    end
  end

  with "#read_frame_with_timeout" do
    it "returns frame when data is available" do
      stream = StringIO.new
      Protocol::FPSS::Frame.new(Protocol::FPSS::MessageType::PING, "\x00").write(stream)
      stream.rewind

      connection = ThetaData::Streaming::Connection.new(stream)

      Sync do
        frame = connection.read_frame_with_timeout(1)
        expect(frame.type).to be == Protocol::FPSS::MessageType::PING
      end
    end

    it "returns nil on EOF" do
      stream = StringIO.new
      connection = ThetaData::Streaming::Connection.new(stream)

      Sync do
        frame = connection.read_frame_with_timeout(1)
        expect(frame).to be == nil
      end
    end

    it "raises TimeoutError when read blocks too long" do
      # Create a pipe where we control when data arrives
      read_io, write_io = IO.pipe

      connection = ThetaData::Streaming::Connection.new(read_io)

      Sync do
        expect {
          connection.read_frame_with_timeout(0.1)
        }.to raise_exception(ThetaData::TimeoutError)
      end
    ensure
      read_io&.close
      write_io&.close
    end

    it "includes timeout duration in error message" do
      read_io, write_io = IO.pipe

      connection = ThetaData::Streaming::Connection.new(read_io)

      Sync do
        begin
          connection.read_frame_with_timeout(0.5)
        rescue ThetaData::TimeoutError => e
          expect(e.message).to be(:include?, "0.5")
        end
      end
    ensure
      read_io&.close
      write_io&.close
    end
  end
end
