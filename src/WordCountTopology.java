import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {

	// 각 component별 ID선언
    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws Exception {
    	
    	// 사용되는 Component선언
        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        /*--------------------토폴로지----------------------*/
        // SentenceSpout --> SplitSentenceBolt --> WordCountBolt --> ReportBolt
        TopologyBuilder builder = new TopologyBuilder();
        
        /* 문장 생성 (1) */
        builder.setSpout(SENTENCE_SPOUT_ID, spout, 4); // Spout가 3개임! 
        /* 문장 나누기(2) */
        builder.setBolt(SPLIT_BOLT_ID, splitBolt, 2)
               .shuffleGrouping(SENTENCE_SPOUT_ID); //셔플그룹핑
        /* 단어세기(3) */
        builder.setBolt(COUNT_BOLT_ID, countBolt, 2)
        	   .fieldsGrouping(SPLIT_BOLT_ID, new Fields("word")); // 필드그룹핑
        /* 결과출력(4) */
        builder.setBolt(REPORT_BOLT_ID, reportBolt)
        	   .globalGrouping(COUNT_BOLT_ID);		// 글로벌그룹핑 

        
        /*--------------------Cluster 설정----------------------*/
        Config config = new Config();
        StormSubmitter cluster = new StormSubmitter();  // StormSubmitter
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
       
    }
}
