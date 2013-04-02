import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFMethodResolver;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * .
 */
@Description(name = "pursway_row_number",
    value = "_FUNC_(partitionByKey1, partitionByKey2 ... ) - assigns a unique number to each row to which it is applied, starting from 1")
   
public class PurswayRowNumberUDF extends UDF {
    private final LongWritable    count        = new LongWritable();
    
    private final StringBuilder            partitionByKeyArgs        = new StringBuilder();    

    private final StringBuilder            keepPartitionByKeys    = new StringBuilder();

    private static final String    NULL        = "NULL";
    private static final String SPLIT        = ";";

    /**
  * 
  */
    public PurswayRowNumberUDF() {
        count.set(0);
    }

    /**
  * @param rslv
  */
    public PurswayRowNumberUDF(UDFMethodResolver rslv) {
        super(rslv);
    }

    /**
  */
    public LongWritable evaluate(Text... args) {
        partitionByKeyArgs.setLength(0);
        
        // partition by keys
        for (int i=0; i< args.length; i++) {
            String arg = args[i].toString();
            if (arg == null) {
                partitionByKeyArgs.append(NULL);
            } else {
                partitionByKeyArgs.append(arg);
            }
            partitionByKeyArgs.append(SPLIT);
        }
        
        // reset counter if we see a new partition
        if (! keepPartitionByKeys.toString().equals(partitionByKeyArgs.toString())) {
            keepPartitionByKeys.setLength(0);
            keepPartitionByKeys.append(partitionByKeyArgs);
            
            count.set(0);
        }
        
        count.set(count.get() + 1);
        return count;
    }
}