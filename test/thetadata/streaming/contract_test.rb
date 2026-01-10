require "thetadata"

describe ThetaData::Streaming::Contract do
  with ".index" do
    let(:contract) { ThetaData::Streaming::Contract.index("SPX") }

    it "creates contract with INDEX sec_type" do
      expect(contract.sec_type).to be == ThetaData::Streaming::SecType::INDEX
    end

    it "stores root symbol" do
      expect(contract.root).to be == "SPX"
    end

    it "has nil expiration" do
      expect(contract.expiration).to be == nil
    end

    it "has nil strike" do
      expect(contract.strike).to be == nil
    end

    it "has nil is_call" do
      expect(contract.is_call).to be == nil
    end
  end

  with ".stock" do
    let(:contract) { ThetaData::Streaming::Contract.stock("AAPL") }

    it "creates contract with STOCK sec_type" do
      expect(contract.sec_type).to be == ThetaData::Streaming::SecType::STOCK
    end

    it "stores root symbol" do
      expect(contract.root).to be == "AAPL"
    end
  end

  with ".option" do
    let(:contract) do
      ThetaData::Streaming::Contract.option(
        root: "AAPL",
        expiration: 20250117,
        strike: 15000,
        is_call: true,
      )
    end

    it "creates contract with OPTION sec_type" do
      expect(contract.sec_type).to be == ThetaData::Streaming::SecType::OPTION
    end

    it "stores root symbol" do
      expect(contract.root).to be == "AAPL"
    end

    it "stores expiration" do
      expect(contract.expiration).to be == 20250117
    end

    it "stores strike" do
      expect(contract.strike).to be == 15000
    end

    it "stores is_call" do
      expect(contract.is_call).to be == true
    end
  end

  with "#to_bytes for simple contracts" do
    it "encodes index contract correctly" do
      contract = ThetaData::Streaming::Contract.index("SPX")
      bytes = contract.to_bytes

      # Format: [total_length][root_length][root_bytes][sec_type]
      # total_length = 3 + "SPX".length = 6
      expect(bytes.getbyte(0)).to be == 6        # total length
      expect(bytes.getbyte(1)).to be == 3        # root length
      expect(bytes[2..4]).to be == "SPX"         # root bytes
      expect(bytes.getbyte(5)).to be == ThetaData::Streaming::SecType::INDEX
    end

    it "encodes stock contract correctly" do
      contract = ThetaData::Streaming::Contract.stock("AAPL")
      bytes = contract.to_bytes

      expect(bytes.getbyte(0)).to be == 7        # total length (3 + 4)
      expect(bytes.getbyte(1)).to be == 4        # root length
      expect(bytes[2..5]).to be == "AAPL"        # root bytes
      expect(bytes.getbyte(6)).to be == ThetaData::Streaming::SecType::STOCK
    end

    it "encodes single-char symbol" do
      contract = ThetaData::Streaming::Contract.stock("A")
      bytes = contract.to_bytes

      expect(bytes.getbyte(0)).to be == 4        # total length
      expect(bytes.getbyte(1)).to be == 1        # root length
      expect(bytes[2]).to be == "A"
    end

    it "encodes long symbol" do
      contract = ThetaData::Streaming::Contract.stock("GOOGL")
      bytes = contract.to_bytes

      expect(bytes.getbyte(0)).to be == 8
      expect(bytes.getbyte(1)).to be == 5
      expect(bytes[2..6]).to be == "GOOGL"
    end

    it "raises for symbol longer than 16 chars" do
      expect {
        ThetaData::Streaming::Contract.new(
          root: "A" * 17,
          sec_type: ThetaData::Streaming::SecType::STOCK,
        )
      }.to raise_exception(ArgumentError)
    end
  end

  with "#to_bytes for option contracts" do
    it "encodes call option correctly" do
      contract = ThetaData::Streaming::Contract.option(
        root: "AAPL",
        expiration: 20250117,
        strike: 15000,
        is_call: true,
      )
      bytes = contract.to_bytes

      # Format: [total_length][root_length][root_bytes][sec_type][exp:4][is_call:1][strike:4]
      # total_length = 12 + "AAPL".length = 16
      expect(bytes.getbyte(0)).to be == 16       # total length
      expect(bytes.getbyte(1)).to be == 4        # root length
      expect(bytes[2..5]).to be == "AAPL"        # root bytes
      expect(bytes.getbyte(6)).to be == ThetaData::Streaming::SecType::OPTION

      # Expiration: 20250117 as big-endian 4 bytes
      exp_bytes = bytes[7..10]
      expect(exp_bytes.unpack1("N")).to be == 20250117

      # is_call: 1 byte
      expect(bytes.getbyte(11)).to be == 1       # true = 1

      # Strike: 15000 as big-endian 4 bytes
      strike_bytes = bytes[12..15]
      expect(strike_bytes.unpack1("N")).to be == 15000
    end

    it "encodes put option correctly" do
      contract = ThetaData::Streaming::Contract.option(
        root: "AAPL",
        expiration: 20250117,
        strike: 15000,
        is_call: false,
      )
      bytes = contract.to_bytes

      expect(bytes.getbyte(11)).to be == 0       # false = 0
    end

    it "encodes different strikes" do
      contract = ThetaData::Streaming::Contract.option(
        root: "SPX",
        expiration: 20250321,
        strike: 550000,  # $5500.00 in cents
        is_call: true,
      )
      bytes = contract.to_bytes

      strike_bytes = bytes[11..14]
      expect(strike_bytes.unpack1("N")).to be == 550000
    end
  end

  with "byte length" do
    it "simple contracts have length = 3 + root.length" do
      contract = ThetaData::Streaming::Contract.index("VIX")
      expect(contract.to_bytes.bytesize).to be == 6
    end

    it "option contracts have length = 12 + root.length" do
      contract = ThetaData::Streaming::Contract.option(
        root: "AAPL",
        expiration: 20250117,
        strike: 15000,
        is_call: true,
      )
      expect(contract.to_bytes.bytesize).to be == 16
    end
  end

  with "validation" do
    it "raises for empty root" do
      expect {
        ThetaData::Streaming::Contract.index("")
      }.to raise_exception(ArgumentError)
    end

    it "raises for nil root" do
      expect {
        ThetaData::Streaming::Contract.new(root: nil, sec_type: ThetaData::Streaming::SecType::STOCK)
      }.to raise_exception(ArgumentError)
    end

    it "raises for option without expiration" do
      expect {
        ThetaData::Streaming::Contract.option(root: "AAPL", expiration: nil, strike: 15000, is_call: true)
      }.to raise_exception(ArgumentError)
    end

    it "raises for option without strike" do
      expect {
        ThetaData::Streaming::Contract.option(root: "AAPL", expiration: 20250117, strike: nil, is_call: true)
      }.to raise_exception(ArgumentError)
    end

    it "raises for option with invalid is_call" do
      expect {
        ThetaData::Streaming::Contract.option(root: "AAPL", expiration: 20250117, strike: 15000, is_call: nil)
      }.to raise_exception(ArgumentError)
    end

    it "raises for negative strike" do
      expect {
        ThetaData::Streaming::Contract.option(root: "AAPL", expiration: 20250117, strike: -100, is_call: true)
      }.to raise_exception(ArgumentError)
    end

    it "raises for zero strike" do
      expect {
        ThetaData::Streaming::Contract.option(root: "AAPL", expiration: 20250117, strike: 0, is_call: true)
      }.to raise_exception(ArgumentError)
    end

    it "raises for strike exceeding UINT32 max" do
      expect {
        ThetaData::Streaming::Contract.option(root: "AAPL", expiration: 20250117, strike: 0xFFFFFFFF + 1, is_call: true)
      }.to raise_exception(ArgumentError)
    end

    it "raises for expiration exceeding UINT32 max" do
      expect {
        ThetaData::Streaming::Contract.option(root: "AAPL", expiration: 0xFFFFFFFF + 1, strike: 15000, is_call: true)
      }.to raise_exception(ArgumentError)
    end
  end

  with "16-byte max root" do
    it "accepts exactly 16-byte root" do
      contract = ThetaData::Streaming::Contract.stock("A" * 16)
      expect(contract.root).to be == "A" * 16
      expect(contract.to_bytes.bytesize).to be == 3 + 16
    end
  end

  with "option encoding round-trip" do
    it "encodes and decodes correctly from raw bytes" do
      contract = ThetaData::Streaming::Contract.option(
        root: "SPY",
        expiration: 20260115,
        strike: 50000,
        is_call: false,
      )
      bytes = contract.to_bytes

      # Manually decode
      total_len = bytes.getbyte(0)
      root_len = bytes.getbyte(1)
      root = bytes[2, root_len]
      sec_type = bytes.getbyte(2 + root_len)
      exp = bytes[(3 + root_len)..(6 + root_len)].unpack1("N")
      call_flag = bytes.getbyte(7 + root_len)
      strike = bytes[(8 + root_len)..(11 + root_len)].unpack1("N")

      expect(total_len).to be == 12 + 3
      expect(root).to be == "SPY"
      expect(sec_type).to be == ThetaData::Streaming::SecType::OPTION
      expect(exp).to be == 20260115
      expect(call_flag).to be == 0
      expect(strike).to be == 50000
    end
  end

  with "encoding format" do
    it "produces ASCII-8BIT encoding" do
      contract = ThetaData::Streaming::Contract.index("SPX")
      expect(contract.to_bytes.encoding).to be == Encoding::ASCII_8BIT
    end
  end

  with ".from_bytes" do
    it "round-trips an index contract" do
      original = ThetaData::Streaming::Contract.index("SPX")
      decoded = ThetaData::Streaming::Contract.from_bytes(original.to_bytes)

      expect(decoded.root).to be == "SPX"
      expect(decoded.sec_type).to be == ThetaData::Streaming::SecType::INDEX
      expect(decoded.expiration).to be == nil
    end

    it "round-trips a stock contract" do
      original = ThetaData::Streaming::Contract.stock("AAPL")
      decoded = ThetaData::Streaming::Contract.from_bytes(original.to_bytes)

      expect(decoded.root).to be == "AAPL"
      expect(decoded.sec_type).to be == ThetaData::Streaming::SecType::STOCK
    end

    it "round-trips an option contract" do
      original = ThetaData::Streaming::Contract.option(
        root: "SPY",
        expiration: 20260115,
        strike: 50000,
        is_call: false,
      )
      decoded = ThetaData::Streaming::Contract.from_bytes(original.to_bytes)

      expect(decoded.root).to be == "SPY"
      expect(decoded.sec_type).to be == ThetaData::Streaming::SecType::OPTION
      expect(decoded.expiration).to be == 20260115
      expect(decoded.strike).to be == 50000
      expect(decoded.is_call).to be == false
    end

    it "round-trips a call option" do
      original = ThetaData::Streaming::Contract.option(
        root: "AAPL",
        expiration: 20250117,
        strike: 15000,
        is_call: true,
      )
      decoded = ThetaData::Streaming::Contract.from_bytes(original.to_bytes)

      expect(decoded.is_call).to be == true
    end
  end

  with "RATE sec_type" do
    it "encodes like a simple contract" do
      contract = ThetaData::Streaming::Contract.new(
        root: "SOFR",
        sec_type: ThetaData::Streaming::SecType::RATE,
      )
      bytes = contract.to_bytes

      expect(bytes.getbyte(0)).to be == 7
      expect(bytes.getbyte(1)).to be == 4
      expect(bytes[2..5]).to be == "SOFR"
      expect(bytes.getbyte(6)).to be == ThetaData::Streaming::SecType::RATE
    end
  end
end

describe ThetaData::Streaming::SecType do
  it "has STOCK = 0" do
    expect(ThetaData::Streaming::SecType::STOCK).to be == 0
  end

  it "has OPTION = 1" do
    expect(ThetaData::Streaming::SecType::OPTION).to be == 1
  end

  it "has INDEX = 2" do
    expect(ThetaData::Streaming::SecType::INDEX).to be == 2
  end

  it "has RATE = 3" do
    expect(ThetaData::Streaming::SecType::RATE).to be == 3
  end

  it "has IGNORE = -1" do
    expect(ThetaData::Streaming::SecType::IGNORE).to be == -1
  end

  with ".name" do
    it "returns constant name for code" do
      expect(ThetaData::Streaming::SecType.name(0)).to be == :STOCK
      expect(ThetaData::Streaming::SecType.name(1)).to be == :OPTION
      expect(ThetaData::Streaming::SecType.name(2)).to be == :INDEX
      expect(ThetaData::Streaming::SecType.name(3)).to be == :RATE
    end

    it "returns nil for unknown code" do
      expect(ThetaData::Streaming::SecType.name(99)).to be == nil
    end
  end
end
