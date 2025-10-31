class P{
                private int i;

                public P(int i) {
                        this.i = i;
                }
                void print() {
                        System.out.println("i="+i);
                }
            void increment() {
                        i++;
                }
}

public class RefTest1 {

        public static void main(String[] args) {
                // TODO Auto-generated method stub
                        P p1=new P(10);
                        P p2=p1;//both the reference variables(p1 and p2) point to the same object
                        p2.increment();
                        p1.print();
                        p2.print();
                
        }

}

