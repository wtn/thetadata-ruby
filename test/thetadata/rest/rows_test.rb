require "thetadata"
require "bigdecimal"
require "time"

describe ThetaData::REST::EODRow do
  let(:row) do
    ThetaData::REST::EODRow.new(
      created: Time.new(2024, 12, 2, 16, 17, 42),
      last_trade: Time.new(2024, 12, 2, 15, 1, 33),
      open: BigDecimal("6040.11"),
      high: BigDecimal("6053.58"),
      low: BigDecimal("6035.33"),
      close: BigDecimal("6047.15"),
      volume: 1000,
      count: 50,
      bid_size: 10,
      bid_exchange: 1,
      bid: BigDecimal("6047.00"),
      bid_condition: 0,
      ask_size: 15,
      ask_exchange: 1,
      ask: BigDecimal("6047.50"),
      ask_condition: 0,
    )
  end

  it "is a Data class" do
    expect(ThetaData::REST::EODRow).to be < Data
  end

  it "has all expected fields" do
    expect(row.open).to be == BigDecimal("6040.11")
    expect(row.high).to be == BigDecimal("6053.58")
    expect(row.low).to be == BigDecimal("6035.33")
    expect(row.close).to be == BigDecimal("6047.15")
    expect(row.volume).to be == 1000
    expect(row.count).to be == 50
  end

  it "is immutable" do
    expect { row.instance_variable_set(:@close, 0) }.to raise_exception(FrozenError)
  end

  it "supports to_h" do
    hash = row.to_h
    expect(hash[:open]).to be == BigDecimal("6040.11")
    expect(hash[:close]).to be == BigDecimal("6047.15")
  end

  it "supports pattern matching" do
    case row
    in ThetaData::REST::EODRow(open:, close:)
      expect(open).to be == BigDecimal("6040.11")
      expect(close).to be == BigDecimal("6047.15")
    else
      raise "Pattern matching failed"
    end
  end
end

describe ThetaData::REST::OHLCRow do
  let(:row) do
    ThetaData::REST::OHLCRow.new(
      timestamp: Time.new(2024, 12, 2, 9, 30, 0),
      open: BigDecimal("6040.11"),
      high: BigDecimal("6047.38"),
      low: BigDecimal("6040.11"),
      close: BigDecimal("6045.92"),
      volume: 500,
      count: 25,
      vwap: BigDecimal("6043.50"),
    )
  end

  it "is a Data class" do
    expect(ThetaData::REST::OHLCRow).to be < Data
  end

  it "has all expected fields" do
    expect(row.timestamp).to be_a(Time)
    expect(row.open).to be == BigDecimal("6040.11")
    expect(row.vwap).to be == BigDecimal("6043.50")
  end

  it "is immutable" do
    expect { row.instance_variable_set(:@close, 0) }.to raise_exception(FrozenError)
  end
end

describe ThetaData::REST::PriceRow do
  let(:row) do
    ThetaData::REST::PriceRow.new(
      timestamp: Time.new(2024, 12, 2, 9, 30, 0),
      price: BigDecimal("6040.11"),
    )
  end

  it "is a Data class" do
    expect(ThetaData::REST::PriceRow).to be < Data
  end

  it "has all expected fields" do
    expect(row.timestamp).to be_a(Time)
    expect(row.price).to be == BigDecimal("6040.11")
  end
end

describe ThetaData::REST::TradeRow do
  let(:row) do
    ThetaData::REST::TradeRow.new(
      timestamp: Time.new(2024, 12, 2, 9, 30, 0),
      sequence: 12345,
      ext_condition1: 32,
      ext_condition2: 255,
      ext_condition3: 1,
      ext_condition4: 115,
      condition: 1,
      size: 100,
      exchange: 7,
      price: BigDecimal("150.25"),
    )
  end

  it "is a Data class" do
    expect(ThetaData::REST::TradeRow).to be < Data
  end

  it "has all expected fields" do
    expect(row.price).to be == BigDecimal("150.25")
    expect(row.size).to be == 100
    expect(row.sequence).to be == 12345
    expect(row.condition).to be == 1
  end
end

describe ThetaData::REST::QuoteRow do
  let(:row) do
    ThetaData::REST::QuoteRow.new(
      timestamp: Time.new(2024, 12, 2, 9, 30, 0),
      bid: BigDecimal("150.00"),
      bid_size: 10,
      bid_exchange: 1,
      bid_condition: 0,
      ask: BigDecimal("150.50"),
      ask_size: 15,
      ask_exchange: 1,
      ask_condition: 0,
    )
  end

  it "is a Data class" do
    expect(ThetaData::REST::QuoteRow).to be < Data
  end

  it "has all expected fields" do
    expect(row.bid).to be == BigDecimal("150.00")
    expect(row.ask).to be == BigDecimal("150.50")
  end

  it "can calculate spread" do
    expect(row.ask - row.bid).to be == BigDecimal("0.50")
  end
end

describe ThetaData::REST::ExpirationRow do
  let(:row) { ThetaData::REST::ExpirationRow.new(symbol: "AAPL", expiration: "20250117") }

  it "is a Data class" do
    expect(ThetaData::REST::ExpirationRow).to be < Data
  end

  it "has symbol and expiration fields" do
    expect(row.symbol).to be == "AAPL"
    expect(row.expiration).to be == "20250117"
  end
end

describe ThetaData::REST::StrikeRow do
  let(:row) { ThetaData::REST::StrikeRow.new(symbol: "AAPL", strike: BigDecimal("150.00")) }

  it "is a Data class" do
    expect(ThetaData::REST::StrikeRow).to be < Data
  end

  it "has symbol and strike fields" do
    expect(row.symbol).to be == "AAPL"
    expect(row.strike).to be == BigDecimal("150.00")
  end
end

describe ThetaData::REST::ContractRow do
  let(:row) do
    ThetaData::REST::ContractRow.new(
      symbol: "AAPL",
      expiration: "20250117",
      strike: BigDecimal("150.00"),
      right: "C",
    )
  end

  it "is a Data class" do
    expect(ThetaData::REST::ContractRow).to be < Data
  end

  it "has all expected fields" do
    expect(row.symbol).to be == "AAPL"
    expect(row.expiration).to be == "20250117"
    expect(row.strike).to be == BigDecimal("150.00")
    expect(row.right).to be == "C"
  end
end

describe ThetaData::REST::SnapshotPriceRow do
  let(:row) do
    ThetaData::REST::SnapshotPriceRow.new(
      timestamp: Time.new(2024, 12, 2, 16, 2, 6),
      symbol: "SPX",
      price: BigDecimal("6047.15"),
    )
  end

  it "is a Data class" do
    expect(ThetaData::REST::SnapshotPriceRow).to be < Data
  end

  it "has all expected fields" do
    expect(row.timestamp).to be_a(Time)
    expect(row.symbol).to be == "SPX"
    expect(row.price).to be == BigDecimal("6047.15")
  end
end

describe ThetaData::REST::SnapshotOHLCRow do
  let(:row) do
    ThetaData::REST::SnapshotOHLCRow.new(
      timestamp: Time.new(2024, 12, 2, 16, 10, 4),
      symbol: "SPX",
      open: BigDecimal("6040.11"),
      high: BigDecimal("6053.58"),
      low: BigDecimal("6035.33"),
      close: BigDecimal("6047.15"),
      volume: 1000,
      count: 50,
    )
  end

  it "is a Data class" do
    expect(ThetaData::REST::SnapshotOHLCRow).to be < Data
  end

  it "has all expected fields" do
    expect(row.timestamp).to be_a(Time)
    expect(row.symbol).to be == "SPX"
    expect(row.open).to be == BigDecimal("6040.11")
  end
end

describe ThetaData::REST::OptionOpenInterestRow do
  let(:row) do
    ThetaData::REST::OptionOpenInterestRow.new(
      symbol: "AAPL",
      expiration: "2024-11-08",
      strike: BigDecimal("220.00"),
      right: "CALL",
      timestamp: Time.new(2024, 12, 2, 9, 30, 0),
      open_interest: 50000,
    )
  end

  it "is a Data class" do
    expect(ThetaData::REST::OptionOpenInterestRow).to be < Data
  end

  it "has all expected fields" do
    expect(row.symbol).to be == "AAPL"
    expect(row.expiration).to be == "2024-11-08"
    expect(row.open_interest).to be == 50000
  end
end
