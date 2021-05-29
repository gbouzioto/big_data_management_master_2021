package project.third_part.helpers;

import java.util.Comparator;

// compares two TeamValue instances based on their team name
public class WomanTeamComparator implements Comparator<TeamValue> {
    public int compare(TeamValue team1, TeamValue team2)
    {
        return team1.getTeam().compareTo(team2.getTeam());
    }
}
