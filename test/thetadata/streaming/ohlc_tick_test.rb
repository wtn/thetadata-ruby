require "thetadata"

describe ThetaData::Streaming::OHLCTick do
  let(:tick) do
    ThetaData::Streaming::OHLCTick.new(
      ms_of_day: 53790000,   # 14:56:30
      open: 550000,
      high: 551000,
      low: 549000,
      close: 550500,
      volume: 1000,
      count: 50,
      price_type: 8,
      date: 20250110,
    )
  end

  with "immutability" do
    it "is a Data class" do
      expect(ThetaData::Streaming::OHLCTick.ancestors).to be(:include?, Data)
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

  with "#open_decimal" do
    it "converts raw open to BigDecimal" do
      expect(tick.open_decimal).to be == BigDecimal("5500.00")
    end
  end

  with "#high_decimal" do
    it "converts raw high to BigDecimal" do
      expect(tick.high_decimal).to be == BigDecimal("5510.00")
    end
  end

  with "#low_decimal" do
    it "converts raw low to BigDecimal" do
      expect(tick.low_decimal).to be == BigDecimal("5490.00")
    end
  end

  with "#close_decimal" do
    it "converts raw close to BigDecimal" do
      expect(tick.close_decimal).to be == BigDecimal("5505.00")
    end
  end

  with "price_type handling" do
    it "returns 0 when price_type is 0" do
      tick = ThetaData::Streaming::OHLCTick.new(
        ms_of_day: 0, open: 100, high: 100, low: 100, close: 100,
        volume: 0, count: 0, price_type: 0, date: 0,
      )
      expect(tick.open_decimal).to be == BigDecimal("0")
    end

    it "handles price_type 10 (whole number)" do
      tick = ThetaData::Streaming::OHLCTick.new(
        ms_of_day: 0, open: 5500, high: 5500, low: 5500, close: 5500,
        volume: 0, count: 0, price_type: 10, date: 0,
      )
      expect(tick.open_decimal).to be == BigDecimal("5500")
    end

    it "handles price_type > 10 (multiply)" do
      tick = ThetaData::Streaming::OHLCTick.new(
        ms_of_day: 0, open: 550, high: 550, low: 550, close: 550,
        volume: 0, count: 0, price_type: 11, date: 0,
      )
      expect(tick.open_decimal).to be == BigDecimal("5500")
    end
  end

  with "#to_h" do
    it "returns hash of all fields" do
      h = tick.to_h
      expect(h[:ms_of_day]).to be == 53790000
      expect(h[:open]).to be == 550000
      expect(h[:high]).to be == 551000
      expect(h[:low]).to be == 549000
      expect(h[:close]).to be == 550500
      expect(h[:volume]).to be == 1000
      expect(h[:count]).to be == 50
      expect(h[:price_type]).to be == 8
      expect(h[:date]).to be == 20250110
    end
  end
end
