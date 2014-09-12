package ninja.mattburgess.drill.pdi.func;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;
import org.pentaho.commons.launcher.Launcher;

import javax.inject.Inject;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * The DrillKettleFuncs class is a collection of functions integrating Drill queries with
 * Pentaho Data Integration (aka PDI aka Kettle). This includes (among other things) the ability to
 * execute a transformation using specified columns as an input dataset.
 */

public class DrillKettleFuncs {

    private DrillKettleFuncs(){}

    @FunctionTemplate( name = "runTrans", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class RunTransformation implements DrillSimpleFunc {

        @Workspace static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunTransformation.class);

        @Param VarCharHolder transName;

        @Output VarCharHolder out;
        @Inject DrillBuf buffer;

        @Workspace private static boolean kettleInitialized = false;

        // Set of dummy workspace objects to ensure the classes are loaded
        @Workspace URLClassLoader dummyUcl;
        @Workspace URL dummyUrl;
        @Workspace Launcher dummyLauncher;

        @Override
        public void setup(RecordBatch incoming) {
            if(!kettleInitialized) {
                try {
                    //KettleEnvironment.init();
                    Launcher.main(null);
                    /*ClassLoader cl = this.getClass().getClassLoader();
                    if(cl instanceof URLClassLoader) {
                        URLClassLoader ucl = (URLClassLoader)cl;
                        URL[] urls = ucl.getURLs();
                        if(urls != null) {
                            for(URL url : urls) {
                                System.out.println(url.toExternalForm());
                            }
                        }
                        else {
                            System.out.println ("No URLs!");
                        }
                    }*/
                    kettleInitialized = true;
                }
                catch(Exception e) {
                    logger.error("Error initializing PDI",e);
                }
            }
        }

        @Override
        public void eval() {
            out.buffer = buffer = buffer.reallocIfNeeded(transName.end - transName.start);
            out.start = 0;
            out.end = transName.end - transName.start;

            StringBuffer sb = new StringBuffer("Ran ");
            sb.append(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(transName.start, transName.end, transName.buffer));

            out.buffer.setBytes(0, sb.toString().getBytes());
            out.buffer.setBytes(0,"WTF".getBytes());

        }
    }
}
