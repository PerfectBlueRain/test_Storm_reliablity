import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

/* 문장 나누기(2) */
public class SplitSentenceBolt extends BaseRichBolt{  //BaseRichBolt
    private OutputCollector collector; // 다음에 처리가 있으면 넘겨주기위해

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    
    // execute() : 실제처리작업을 수행하는 함수
    public void execute(Tuple tuple) {
        String sentence = tuple.getStringByField("sentence");// tuple(넘겨준데이터)에서 key로 value를 뽑음
        String[] words = sentence.split(" ");
        for(String word : words){
            this.collector.emit(new Values(word)); // OutputCollector의 emit()로 다음으로 내보냄(1)
        }
        this.collector.ack(tuple); // 성공시에 알려줌
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));  // 다음에 넘겨줄 Fields의 key -"word"를 선언해준다!(2)
    }
}
