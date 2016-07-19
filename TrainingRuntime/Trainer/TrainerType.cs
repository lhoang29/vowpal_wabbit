namespace Microsoft.Content.Recommendations.TrainingRuntime.Trainer
{
    public enum TrainerType
    {
        NOP,

        CBDR,

        CBADF,
    }

    public static class TrainerOptions
    {
        public const string Actions = "actions";

        public const string Interact = "iact";

        public const string Custom = "ctm";

        public const string CustomResume = "ctmr";

        public const string GenerateModelId = "gmid";
    }
}
