package project.third_part;

import java.util.Comparator;

public class WomanTeamComparator implements Comparator<TeamValue> {
    public int compare(TeamValue team1, TeamValue team2)
    {
        return team1.getTeam().compareTo(team2.getTeam());
    }
}
