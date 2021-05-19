package project.second_part;

import java.util.Comparator;

public class TopAthleteKeyComparator implements Comparator<AthleteKey> {
    public int compare(AthleteKey at1, AthleteKey at2)
    {
        int result = at1.getGold().compareTo(at2.getGold());
        if (result == 0) {
            result = at1.getTotalMedals().compareTo(at2.getTotalMedals());
        }
        return result;
    }
}
