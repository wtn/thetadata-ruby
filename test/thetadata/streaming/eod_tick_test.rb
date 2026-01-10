require "thetadata"

describe ThetaData::Streaming::EODTick do
  let(:tick) do
    ThetaData::Streaming::EODTick.new(
      ms_of_day: 53790000,     # 14:56:30 - created time
      ms_of_day2: 57600000,    # 16:00:00 - last trade time
      open: 550000,
      high: 551000,
      low: 549000,
      close: 550500,
      volume: 1000000,
      count: 5000,
      bid_size: 100,
      bid_exchange: 1,
      bid: 550400,
      bid_condition: 0,
      ask_size: 200,
      ask_exchange: 2,
      ask: 550600,
      ask_condition: 0,
      price_type: 8,
      date: 20250110,
    )
  end

  with "immutability" do
    it "is a Data class" do
      expect(ThetaData::Streaming::EODTick.ancestors).to be(:include?, Data)
    end

    it "is frozen" do
      expect(tick.frozen?).to be == true
    end
  end

  with "#time" do
    it "formats ms_of_day as HH:MM:SS.mmm" do
      expect(tick.time).to be == "14:56:30.000"
    end
  end

  with "#last_trade_time" do
    it "formats ms_of_day2 as HH:MM:SS.mmm" do
      expect(tick.last_trade_time).to be == "16:00:00.000"
    end
  end

  with "OHLC decimals" do
    it "converts open to BigDecimal" do
      expect(tick.open_decimal).to be == BigDecimal("5500.00")
    end

    it "converts high to BigDecimal" do
      expect(tick.high_decimal).to be == BigDecimal("5510.00")
    end

    it "converts low to BigDecimal" do
      expect(tick.low_decimal).to be == BigDecimal("5490.00")
    end

    it "converts close to BigDecimal" do
      expect(tick.close_decimal).to be == BigDecimal("5505.00")
    end
  end

  with "quote decimals" do
    it "converts bid to BigDecimal" do
      expect(tick.bid_decimal).to be == BigDecimal("5504.00")
    end

    it "converts ask to BigDecimal" do
      expect(tick.ask_decimal).to be == BigDecimal("5506.00")
    end
  end

  with "#mid_decimal" do
    it "returns average of bid and ask" do
      expect(tick.mid_decimal).to be == BigDecimal("5505.00")
    end
  end

  with "#spread_decimal" do
    it "returns ask minus bid" do
      expect(tick.spread_decimal).to be == BigDecimal("2.00")
    end
  end

  with "#to_h" do
    it "returns hash of all fields" do
      h = tick.to_h
      expect(h[:ms_of_day]).to be == 53790000
      expect(h[:ms_of_day2]).to be == 57600000
      expect(h[:open]).to be == 550000
      expect(h[:high]).to be == 551000
      expect(h[:low]).to be == 549000
      expect(h[:close]).to be == 550500
      expect(h[:volume]).to be == 1000000
      expect(h[:count]).to be == 5000
      expect(h[:bid_size]).to be == 100
      expect(h[:bid_exchange]).to be == 1
      expect(h[:bid]).to be == 550400
      expect(h[:bid_condition]).to be == 0
      expect(h[:ask_size]).to be == 200
      expect(h[:ask_exchange]).to be == 2
      expect(h[:ask]).to be == 550600
      expect(h[:ask_condition]).to be == 0
      expect(h[:price_type]).to be == 8
      expect(h[:date]).to be == 20250110
    end
  end
end
