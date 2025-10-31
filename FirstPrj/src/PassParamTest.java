class Sample{
                private int i;

                public Sample(int i) {
                
                        this.i = i;
                }
                void increment() {
                        i++;
                }
                void print() {
                        System.out.println("i="+i);
                }
                
}

class Next{
                void change(int i) {
                        i++;
                }
                
                void change(Sample s1) {
                        s1.increment();
                }
}

public class PassParamTest {
public static void main(String[] args) {
        int x=10;
        Sample s=new Sample(10);
        Next nxt=new Next();
        nxt.change(x);//value 10 is passed
        nxt.change(s);//the reference of the object is passed
        System.out.println(x);
        s.print();
}
}


