package edu.zju.soft.jzs.service;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by 11544 on 2017/11/17.
 */
public class TransData {

    /**
     * 将文件转为hashmap
     *
     * @param filename
     * @return
     * @throws Exception
     */
    public static LinkedHashMap<String, Long> transFileToMap(String filename) throws Exception {
        File file = new File(filename);
        if (!file.exists()) {
            System.out.println("找不到文件");
            throw new RuntimeException("File not found");
        }
        LinkedHashMap<String, Long> map = new LinkedHashMap<String, Long>();//HashMap<String, Long>();
        Scanner in = new Scanner(file, "UTF-8");

        while (in.hasNextLine()) {
            String line = in.nextLine();
            String[] datas = line.split("\t");
            long value = Long.parseLong(datas[1]);
            if (value < 20) {
                break;
            }
            //将每个词之间的距离拉大所以乘以了9973(一万以内的质数)
            map.put(datas[0], value * 9973);
        }



        return map;
    }

    /**
     * 这个运行效率会很高的
     * @param inputfile
     * @param outputfile
     * @param transmap
     * @return
     */
    public static boolean transDataToVector(String inputfile, String outputfile, LinkedHashMap<String, Long> transmap) {
        System.out.println("开始转换数据");
        long lineNumber = 0;
        try {

            File file = new File(inputfile);
            Scanner in = new Scanner(file, "UTF-8");
            PrintWriter out = new PrintWriter(outputfile, "UTF-8");
            while (in.hasNextLine()) {
                lineNumber++;
                String line = in.nextLine();
                String[] datas = line.split("\t");
                String keyword = datas[2];

                List<Word> keywordSplitData = WordSegmenter.seg(keyword);

                long alllocation = 0;
                //对词进行分离 加权
                for (Word word : keywordSplitData) {
                    Long X = transmap.get(word.toString());

                    long xdata = 340;
                    if (X != null) {
                        xdata = X;
                    }

                    alllocation += xdata;
                }
                //取他这个词的位置的平均值
                long location = 0;
                if (keywordSplitData.size() != 0) {
                    location = alllocation / keywordSplitData.size();
                }
                String newLine = keyword + '\t' + location + '\t' + location;
                out.println(newLine);
                out.flush();
            }
            out.close();
            System.out.println(inputfile + "分解完成\n" + "目标路径为:" + outputfile);
            return true;
        } catch (IOException e) {
            System.out.println("在" + lineNumber + "行中有异常");
            e.printStackTrace();
        }
        return false;
    }



}
