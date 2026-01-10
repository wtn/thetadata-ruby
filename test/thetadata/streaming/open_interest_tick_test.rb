require "thetadata"

describe ThetaData::Streaming::OpenInterestTick do
  let(:tick) do
    ThetaData::Streaming::OpenInterestTick.new(
      ms_of_day: 53790000,   # 14:56:30
      open_interest: 12500,
      date: 20250110,
    )
  end

  with "immutability" do
    it "is a Data class" do
      expect(ThetaData::Streaming::OpenInterestTick.ancestors).to be(:include?, Data)
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

  with "accessors" do
    it "returns open_interest" do
      expect(tick.open_interest).to be == 12500
    end

    it "returns date" do
      expect(tick.date).to be == 20250110
    end
  end

  with "#to_h" do
    it "returns hash of all fields" do
      h = tick.to_h
      expect(h[:ms_of_day]).to be == 53790000
      expect(h[:open_interest]).to be == 12500
      expect(h[:date]).to be == 20250110
    end
  end
end
