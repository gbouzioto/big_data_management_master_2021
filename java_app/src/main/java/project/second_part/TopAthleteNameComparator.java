package project.second_part;

import java.util.Comparator;

public class TopAthleteNameComparator implements Comparator<TopAthlete> {
    public int compare(TopAthlete at1, TopAthlete at2)
    {
        return at1.getName().compareTo(at2.getName());
    }
}
