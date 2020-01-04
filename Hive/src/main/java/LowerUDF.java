import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author: YangFei
 * @description: Hive测试UDF函数使用，UDF实现数据库中字段的字符串大写转换小写功能
 * @create:2020-01-01 15:33
 */
public class LowerUDF extends UDF {
    /**
     * 1. Implement one or more methods named "evaluate" which will be called by Hive.
     * <p>
     * 2. "evaluate" should never be a void method. However it can return "null" if needed.
     */
    public Text evaluate(Text text) {
        // input parameter validate
        if (null == text) {
            return null;
        }

        // validate
        if (StringUtils.isBlank(text.toString())) {
            return null;
        }

        // lower
        return new Text(text.toString().toLowerCase());
    }
}
