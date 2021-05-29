package project.second_part.helpers;

import lombok.Data;
import lombok.experimental.Accessors;

/*
Converts a TopAthleteKeyWritable to an AthleteKey in order to be used in intermediate data structures.
Hadoop objects are optimized in way such that their bytes cannot be written more than once in other data structures.
Therefore, this conversion is used.
 */
@Data
@Accessors(chain = true)
public class AthleteKey {
    private String name;
    private Integer gold;
    private Integer totalMedals;

    public AthleteKey (TopAthleteKeyWritable athletesKeyWritable) {
        this.name = athletesKeyWritable.getName().toString();
        this.gold = athletesKeyWritable.getGold().get();
        this.totalMedals = athletesKeyWritable.getTotalMedals().get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AthleteKey that = (AthleteKey) o;
        return that.gold.equals(this.gold) && that.totalMedals.equals(this.totalMedals);
    }
}
