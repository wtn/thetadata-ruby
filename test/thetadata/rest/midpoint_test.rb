require "thetadata"

describe ThetaData::REST do
  with ".midpoint_value" do
    # Tests based on Java implementation:
    # return (bid / 2) + (ask / 2) + ((bid % 2 + ask % 2) / 2)

    it "calculates midpoint for two even numbers" do
      # 100/2 + 200/2 + (0+0)/2 = 50 + 100 + 0 = 150
      expect(ThetaData::REST.midpoint_value(100, 200)).to be == 150
    end

    it "calculates midpoint for two odd numbers" do
      # 101/2 + 201/2 + (1+1)/2 = 50 + 100 + 1 = 151
      expect(ThetaData::REST.midpoint_value(101, 201)).to be == 151
    end

    it "calculates midpoint for even bid, odd ask (rounds down)" do
      # 100/2 + 201/2 + (0+1)/2 = 50 + 100 + 0 = 150
      expect(ThetaData::REST.midpoint_value(100, 201)).to be == 150
    end

    it "calculates midpoint for odd bid, even ask (rounds down)" do
      # 101/2 + 200/2 + (1+0)/2 = 50 + 100 + 0 = 150
      expect(ThetaData::REST.midpoint_value(101, 200)).to be == 150
    end

    it "calculates midpoint for equal values" do
      expect(ThetaData::REST.midpoint_value(100, 100)).to be == 100
    end

    it "calculates midpoint when bid is zero" do
      expect(ThetaData::REST.midpoint_value(0, 100)).to be == 50
    end

    it "calculates midpoint when ask is zero" do
      expect(ThetaData::REST.midpoint_value(100, 0)).to be == 50
    end

    it "calculates midpoint for large values" do
      # Verify no overflow issues (Ruby handles big integers natively)
      bid = 1_000_000_000
      ask = 2_000_000_000
      expect(ThetaData::REST.midpoint_value(bid, ask)).to be == 1_500_000_000
    end

    it "calculates midpoint for decimal values" do
      # Prices come as BigDecimal, midpoint should work with them
      bid = BigDecimal("154.45")
      ask = BigDecimal("154.55")
      result = ThetaData::REST.midpoint_value(bid, ask)
      expect(result).to be == BigDecimal("154.50")
    end

    it "matches simple average for even sum" do
      # When bid + ask is even, result should equal (bid + ask) / 2
      expect(ThetaData::REST.midpoint_value(100, 200)).to be == (100 + 200) / 2
    end

    it "rounds down for odd sum (matches Java integer division)" do
      # When bid + ask is odd, result should round down like Java
      # (100 + 201) / 2 = 150.5 → 150 (integer division)
      expect(ThetaData::REST.midpoint_value(100, 201)).to be == 150
    end
  end
end
