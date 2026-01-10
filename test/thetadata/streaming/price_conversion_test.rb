require "thetadata"

# Standalone tester to test the module directly
class PriceConversionTester
  include ThetaData::Streaming::PriceConversion
  public :price_to_decimal
end

describe ThetaData::Streaming::PriceConversion do
  let(:converter) { PriceConversionTester.new }

  with "price_type 0 (no conversion)" do
    it "returns 0 regardless of price" do
      expect(converter.price_to_decimal(0, 0)).to be == BigDecimal("0")
      expect(converter.price_to_decimal(999999, 0)).to be == BigDecimal("0")
      expect(converter.price_to_decimal(-100, 0)).to be == BigDecimal("0")
    end
  end

  with "price_type 1 (9 decimal places)" do
    it "divides by 10^9" do
      expect(converter.price_to_decimal(1_000_000_000, 1)).to be == BigDecimal("1")
      expect(converter.price_to_decimal(123456789, 1)).to be == BigDecimal("0.123456789")
    end
  end

  with "price_type 2 (8 decimal places)" do
    it "divides by 10^8" do
      expect(converter.price_to_decimal(100_000_000, 2)).to be == BigDecimal("1")
    end
  end

  with "price_type 3 (7 decimal places)" do
    it "divides by 10^7" do
      expect(converter.price_to_decimal(10_000_000, 3)).to be == BigDecimal("1")
    end
  end

  with "price_type 4 (6 decimal places)" do
    it "divides by 10^6" do
      expect(converter.price_to_decimal(1_000_000, 4)).to be == BigDecimal("1")
    end
  end

  with "price_type 5 (5 decimal places)" do
    it "divides by 10^5" do
      expect(converter.price_to_decimal(100_000, 5)).to be == BigDecimal("1")
    end
  end

  with "price_type 6 (4 decimal places)" do
    it "divides by 10^4" do
      expect(converter.price_to_decimal(123456, 6)).to be == BigDecimal("12.3456")
    end
  end

  with "price_type 7 (3 decimal places)" do
    it "divides by 10^3" do
      expect(converter.price_to_decimal(123456, 7)).to be == BigDecimal("123.456")
    end
  end

  with "price_type 8 (2 decimal places, most common)" do
    it "divides by 100" do
      expect(converter.price_to_decimal(550564, 8)).to be == BigDecimal("5505.64")
    end

    it "handles typical equity prices" do
      expect(converter.price_to_decimal(15000, 8)).to be == BigDecimal("150.00")
      expect(converter.price_to_decimal(19999, 8)).to be == BigDecimal("199.99")
    end

    it "handles typical index prices" do
      expect(converter.price_to_decimal(553020, 8)).to be == BigDecimal("5530.20")
    end
  end

  with "price_type 9 (1 decimal place)" do
    it "divides by 10" do
      expect(converter.price_to_decimal(5505, 9)).to be == BigDecimal("550.5")
    end
  end

  with "price_type 10 (whole number)" do
    it "returns price as-is" do
      expect(converter.price_to_decimal(5500, 10)).to be == BigDecimal("5500")
      expect(converter.price_to_decimal(0, 10)).to be == BigDecimal("0")
    end
  end

  with "price_type 11 (multiply by 10)" do
    it "multiplies by 10" do
      expect(converter.price_to_decimal(550, 11)).to be == BigDecimal("5500")
    end
  end

  with "price_type 12 (multiply by 100)" do
    it "multiplies by 100" do
      expect(converter.price_to_decimal(55, 12)).to be == BigDecimal("5500")
    end
  end

  with "edge cases" do
    it "handles zero price with non-zero price_type" do
      expect(converter.price_to_decimal(0, 8)).to be == BigDecimal("0")
      expect(converter.price_to_decimal(0, 10)).to be == BigDecimal("0")
      expect(converter.price_to_decimal(0, 11)).to be == BigDecimal("0")
    end

    it "handles negative prices" do
      expect(converter.price_to_decimal(-100, 8)).to be == BigDecimal("-1.00")
      expect(converter.price_to_decimal(-5500, 10)).to be == BigDecimal("-5500")
    end

    it "handles very large prices" do
      expect(converter.price_to_decimal(999_999_999, 8)).to be == BigDecimal("9999999.99")
    end
  end

  with "included in TradeTick" do
    it "converts price using the module" do
      tick = ThetaData::Streaming::TradeTick.new
      tick.apply_changes([100, 0, 0, 0, 0, 550564, 0, 8, 0])
      expect(tick.price_decimal).to be == BigDecimal("5505.64")
    end
  end

  with "included in QuoteTick" do
    it "converts bid and ask using the module" do
      quote = ThetaData::Streaming::QuoteTick.new
      quote.apply_changes([100, 0, 0, 0, 15000, 0, 0, 0, 15100, 0, 8, 0])
      expect(quote.bid_decimal).to be == BigDecimal("150.00")
      expect(quote.ask_decimal).to be == BigDecimal("151.00")
    end
  end

  with "included in OHLCTick" do
    it "converts OHLC prices using the module" do
      tick = ThetaData::Streaming::OHLCTick.new(
        ms_of_day: 34200000,
        open: 450000,
        high: 451000,
        low: 449000,
        close: 450500,
        volume: 10000,
        count: 500,
        price_type: 8,
        date: 20250110,
      )
      expect(tick.open_decimal).to be == BigDecimal("4500.00")
      expect(tick.high_decimal).to be == BigDecimal("4510.00")
      expect(tick.low_decimal).to be == BigDecimal("4490.00")
      expect(tick.close_decimal).to be == BigDecimal("4505.00")
    end
  end

  with "included in EODTick" do
    it "converts all price fields using the module" do
      tick = ThetaData::Streaming::EODTick.new(
        ms_of_day: 57600000,
        ms_of_day2: 57599000,
        open: 450000,
        high: 451000,
        low: 449000,
        close: 450500,
        volume: 100000,
        count: 5000,
        bid_size: 100,
        bid_exchange: 1,
        bid: 450400,
        bid_condition: 0,
        ask_size: 200,
        ask_exchange: 2,
        ask: 450600,
        ask_condition: 0,
        price_type: 8,
        date: 20250110,
      )
      expect(tick.open_decimal).to be == BigDecimal("4500.00")
      expect(tick.close_decimal).to be == BigDecimal("4505.00")
      expect(tick.bid_decimal).to be == BigDecimal("4504.00")
      expect(tick.ask_decimal).to be == BigDecimal("4506.00")
      expect(tick.mid_decimal).to be == BigDecimal("4505.00")
      expect(tick.spread_decimal).to be == BigDecimal("2.00")
    end
  end
end
