import { PoolClient } from 'pg';

export default class AsyncJobRunner<
  TJob extends { id: string; companyId: string; retry: number; type: string },
  TJobInput
> {
  timeout: NodeJS.Timeout | null = null;
  tickRate: number;
  halted = false;

  queryJob: () => Promise<TJob | null>;
  insertJob: (data: TJobInput, client?: PoolClient) => Promise<TJob>;
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
    tickRate?: number;
  }) {
    this.queryJob = args.queryJob;
    this.insertJob = args.insertJob;
    this.processJob = args.processJob;
    this.onJobFailed = args.onJobFailed;
    this.onJobCompleted = args.onJobCompleted;
    this.logJob = args.logJob;
    this.tickRate = args.tickRate || 5000;
  }

  log = (message: string, job?: TJob) => {
    if (!this.logJob) return;

    if (job) {
      this.logJob(`Job ${job.id}: ${message}`);
    } else {
      this.logJob(message);
    }
  };

  halt = (): boolean => {
    this.log('halt()');
    this.halted = true;
    return this.halted;
  };

  resume = (): boolean => {
    this.log('resume()');
    this.halted = false;
    this.getJob();
    return !this.halted;
  };

  idle = (): void => {
    this.log('idle()');
    this.timeout = setTimeout(() => {
      this.timeout = null;
      this.getJob();
    }, this.tickRate);
  };

  getJob = async () => {
    if (this.timeout) {
      return;
    }

    if (this.halted) {
      this.log('getJob: halted');
      return;
    }

    this.log('getJob()');

    const job = await this.queryJob();

    if (!job) {
      this.idle();
    } else {
      this.runJob(job);
    }
  };

  runJob = async (job: TJob) => {
    this.log('runJob()', job);
    let succeeded: boolean = false;

    try {
      succeeded = await this.processJob(job);
      this.log(succeeded ? 'completed' : 'not completed', job);
      await this.onJobCompleted(job, succeeded);
    } catch (error) {
      this.log(`runJob: job failed ${JSON.stringify({ error })}`, job);
      await this.onJobFailed(job, error);
    } finally {
      if (succeeded) {
        this.getJob();
      } else {
        this.idle();
      }
    }
  };

  createJob = async (data: TJobInput, client?: PoolClient) => {
    this.log('createJob()');
    const job = await this.insertJob(data, client);
    this.getJob();
    return job;
  };
}
