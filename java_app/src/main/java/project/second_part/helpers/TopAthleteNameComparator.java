package project.second_part.helpers;

import java.util.Comparator;

// compares TopAthlete instances based on the athlete's name, used for sorting purposes.
public class TopAthleteNameComparator implements Comparator<TopAthlete> {
    public int compare(TopAthlete at1, TopAthlete at2)
    {
        return at1.getName().compareTo(at2.getName());
    }
}
