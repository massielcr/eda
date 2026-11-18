namespace RPCServer
{
    public static class MathHelper
    {
        public static int Fibonnaci(int n)
        {
            int a = 0;
            int b = 1;

            for (int i = 0; i < n; i++)
            {
                (a, b) = (b, a + b);
            }

            return a;
        }
    }
}
