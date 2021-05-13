package project;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import static java.lang.Integer.parseInt;

@Data
@Accessors(chain = true)
public class Athlete {

    private IntWritable id;
    private Text name;
    private Text sex;
    private Text age;
    private Text height;
    private Text weight;
    private Text team;
    private Text noc;
    private Text games;
    private Text year;
    private Text season;
    private Text city;
    private Text sport;
    private Text event;
    private Text medal;

    public Athlete(String[] line) {
        this.id = new IntWritable(parseInt(line[Constants.ID]));
        this.name = new Text(line[Constants.NAME]);
        this.sex = new Text(line[Constants.SEX]);
        this.age = new Text(line[Constants.AGE]);
        this.height = new Text(line[Constants.HEIGHT]);
        this.weight = new Text(line[Constants.WEIGHT]);
        this.team = new Text(line[Constants.TEAM]);
        this.noc = new Text(line[Constants.NOC]);
        this.games = new Text(line[Constants.GAMES]);
        this.year = new Text(line[Constants.YEAR]);
        this.season = new Text(line[Constants.SEASON]);
        this.city = new Text(line[Constants.CITY]);
        this.sport = new Text(line[Constants.SPORT]);
        this.event = new Text(line[Constants.EVENT]);
        this.medal = new Text(line[Constants.MEDAL]);
    }
}
