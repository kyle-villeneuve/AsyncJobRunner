class AsyncJobRunner<TJob extends { id: string; companyId: string; retry: number; type: string }, TJobInput> {
  currentJob: null | TJob = null;
  timeout: NodeJS.Timeout | null = null;
  tickRate: number = 5000;

  queryJob: () => Promise<TJob | null>;
  insertJob: (data: TJobInput) => Promise<TJob>;
  processJob: (data: TJob) => Promise<boolean>;
  onJobFailed: (data: TJob, error: Error) => Promise<any>;
  onJobCompleted: (data: TJob, completed: boolean) => Promise<any>;
  logJob?: (message: string) => void;

  constructor(args: {
    insertJob: (data: TJobInput) => Promise<TJob>;
    queryJob: () => Promise<TJob | null>;
    processJob: (data: TJob) => Promise<boolean>;
    onJobCompleted: (data: TJob, completed: boolean) => Promise<any>;
    onJobFailed: (data: TJob, error: Error) => Promise<any>;
    logJob?: (message: string) => void;
  }) {
    this.queryJob = args.queryJob;
    this.insertJob = args.insertJob;
    this.processJob = args.processJob;
    this.onJobFailed = args.onJobFailed;
    this.onJobCompleted = args.onJobCompleted;
    this.logJob = args.logJob;
  }

  log(message: string) {
    if (!this.currentJob) {
      this.logJob?.(message);
    } else {
      this.logJob?.(`${this.currentJob.id}: ${message}`);
    }
  }

  idle = () => {
    this.log('idle');

    if (this.timeout) {
      clearTimeout(this.timeout);
      this.timeout = null;
    }
  };

  tick = () => {
    if (this.timeout) return;

    this.log('tick');
    this.timeout = setTimeout(this.getJob, this.tickRate);
  };

  getJob = async () => {
    this.log('getJob');
    this.currentJob = await this.queryJob();

    if (!this.currentJob) {
      this.idle();
    } else {
      this.runJob();
    }
  };

  runJob = async () => {
    if (!this.currentJob) return null;
    this.log('started');

    try {
      const completed = await this.processJob(this.currentJob);
      this.log(completed ? 'completed' : 'not completed');
      await this.onJobCompleted(this.currentJob, completed);
    } catch (error) {
      await this.onJobFailed(this.currentJob, error);
      this.log('failed');
    } finally {
      this.currentJob = null;
      this.timeout = null;
      this.tick();
    }
  };

  createJob = async (data: TJobInput) => {
    const job = this.insertJob(data);
    this.tick();
    return job;
  };
}
