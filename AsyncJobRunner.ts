import { PoolClient } from 'pg';

export default class AsyncJobRunner<
  TJob extends { id: string; companyId: string; retry: number; type: string },
  TJobInput
> {
  currentJob: null | TJob = null;
  timeout: NodeJS.Timeout | null = null;
  tickRate = 5000;
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

  halt = () => {
    this.log('halt()');
    this.halted = true;
    return this.halted;
  };

  resume = () => {
    this.log('resume()');
    if (this.halted){
      this.getJob();
    }
    this.halted = false;
    return this.halted;
  }

  idle = () => {
    this.log('idle()');
    this.timeout = setTimeout(this.getJob, this.tickRate);
  };

  purge = () => {
    this.log('purge()');
    if (this.timeout) {
      clearTimeout(this.timeout);
      this.timeout = null;
    }
    if (this.currentJob) {
      this.currentJob = null;
    }
  };

  getJob = async () => {
    if (this.halted) return;
    this.log('getJob()');

    if (this.currentJob) {
      this.log('getJob: job exists');
      return;
    }

    this.currentJob = await this.queryJob();

    if (!this.currentJob) {
      this.idle();
    } else {
      await this.runJob();
    }
  };

  runJob = async () => {
    this.log('runJob()');
    if (!this.currentJob) {
      return null;
    }

    let succeeded = false;

    try {
      succeeded = await this.processJob(this.currentJob);
      this.log(succeeded ? 'completed' : 'not completed');
      await this.onJobCompleted(this.currentJob, succeeded);
    } catch (error) {
      this.log(`runJob: job failed ${JSON.stringify({ error })}`);
      await this.onJobFailed(this.currentJob, error);
    } finally {
      this.purge();

      if (succeeded) {
        await this.getJob();
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
