package ninja.mattburgess.drill.pdi.func;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;

/**
 * Created by mburgess on 9/11/14.
 */

public class DrillKettleFuncs {

    @FunctionTemplate(
            name = "krun",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class RunTransformation implements DrillSimpleFunc {

        @Override
        public void setup(RecordBatch incoming) {

        }

        @Override
        public void eval() {

        }
    }

}
