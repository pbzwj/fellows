import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSDemon1 {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration=new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:8020"),configuration,"root");
        //fileSystem.copyFromLocalFile(new Path("d://xiyou.txt"),new Path("/user/root/input"));
        //fileSystem.copyToLocalFile(new Path("/user/root/input/shuihu.txt"),new Path("d://haha.txt"));
        //fileSystem.mkdirs(new Path("/user/root/output/aa/bb"));
        //fileSystem.delete(new Path("/user/root/output"),true);
        //fileSystem.rename(new Path("/user/root/input/xiyou.txt"),new Path("/user/root/input/buzhidao.txt"));
        RemoteIterator<LocatedFileStatus> lf = fileSystem.listFiles(new Path("/"), true);
        while(lf.hasNext()){
            LocatedFileStatus next = lf.next();
        }
        fileSystem.close();
    }
}
