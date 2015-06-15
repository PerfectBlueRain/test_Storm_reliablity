import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/*���� ���� (1)*/
public class SentenceSpout extends BaseRichSpout { 

	private SpoutOutputCollector collector; 
    private int index = 0;
    
    /*��Ȯ�� Ʃ���� �����ϱ� ���� �ؽø�*/
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
        this.pending = new ConcurrentHashMap<UUID, Values>(); //�ʱ�ȭ
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence")); 
    }

    public void nextTuple() {
        Values values = new Values(sentences[index]);
        UUID msgId = UUID.randomUUID();
        this.pending.put(msgId, values); // msgId �ӽ÷� �����ϴ� �κ�
        this.collector.emit(values, msgId); // msgId�� ����
        index++;
        if (index >= sentences.length) {
            index = 0;
        }
        Utils.waitForMillis(1);
    }
    
    @Override
    /* ��Ʈ���� ó���Ϸ� �޼����� Ȯ���� */
    public void ack(Object msgId) {
        this.pending.remove(msgId); // �ӽ÷� ����� msgId�� ����
    }
    @Override
    /* ��Ʈ���� ������ ��� �ٽ� ���� */
    public void fail(Object msgId) {
        this.collector.emit(this.pending.get(msgId), msgId); // msgId�� �ٽ� ����
    } 

    
}
