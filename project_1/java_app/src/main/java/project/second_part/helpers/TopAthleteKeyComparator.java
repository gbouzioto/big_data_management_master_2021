package project.second_part.helpers;

import java.util.Comparator;

/*
Comparator used to compare Athlete keys. For the second part of the project, an athlete with the most gold medals
is placed higher in the ranking. If the gold medals tie, then the total medals are compared. If these tie as well,
then the athletes have the same ranking.
*/
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
