import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/*문장 생성 (1)*/
public class SentenceSpout extends BaseRichSpout { 

	private SpoutOutputCollector collector; 
    private int index = 0;
    
    /*미확인 튜플을 보관하기 위한 해시맵*/
    private ConcurrentHashMap<UUID, Values> pending;
    
    private String[] sentences = {
        "my dog has fleas",
        "i like cold beverages",
        "the dog ate my homework",
        "don't have a cow man",
        "i don't think i like fleas"
    };

    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.pending = new ConcurrentHashMap<UUID, Values>(); //초기화
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence")); 
    }

    public void nextTuple() {
        Values values = new Values(sentences[index]);
        UUID msgId = UUID.randomUUID();
        this.pending.put(msgId, values); // msgId 임시로 저장하는 부분
        this.collector.emit(values, msgId); // msgId를 전송
        index++;
        if (index >= sentences.length) {
            index = 0;
        }
        Utils.waitForMillis(1);
    }
    
    @Override
    /* 볼트에서 처리완료 메세지를 확인함 */
    public void ack(Object msgId) {
        this.pending.remove(msgId); // 임시로 저장된 msgId는 삭제
    }
    @Override
    /* 볼트에서 실패한 경우 다시 보냄 */
    public void fail(Object msgId) {
        this.collector.emit(this.pending.get(msgId), msgId); // msgId를 다시 전송
    } 

    
}
