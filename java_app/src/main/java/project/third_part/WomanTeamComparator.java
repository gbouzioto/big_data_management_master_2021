package project.third_part;

import java.util.Comparator;

public class WomanTeamComparator implements Comparator<TeamValueWritable> {
    public int compare(TeamValueWritable team1, TeamValueWritable team2)
    {
        return team1.getTeam().compareTo(team2.getTeam());
    }
}
