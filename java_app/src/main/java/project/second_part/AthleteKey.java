package project.second_part;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class AthleteKey {
    private String name;
    private Integer gold;
    private Integer totalMedals;

    public AthleteKey (TopAthletesKeyWritable athletesKeyWritable) {
        this.name = athletesKeyWritable.getName().toString();
        this.gold = athletesKeyWritable.getGold().get();
        this.totalMedals = athletesKeyWritable.getTotalMedals().get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AthleteKey that = (AthleteKey) o;
        return that.getGold().equals(this.gold) && that.getTotalMedals().equals(this.totalMedals);
    }
}
