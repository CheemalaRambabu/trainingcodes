class A{
        private int i;
        private int j;
        
        A(int i,int j){
              this.i=i;
               this.j=j;
        }
        
        void print() {
                System.out.println("i="+i+"\tj="+j);
        }
}

public class ThisTest {
        public static void main(String[] args) {
                A a1=new A(10,20);
                A a2=new A(4,8);
                a1.print();
                a2.print();
        }

}


