import {
  Body,
  Controller,
  Get,
  HttpException,
  HttpStatus,
  Inject,
  Post,
} from '@nestjs/common';
import { AppService } from './app.service';
import { ClientKafka } from '@nestjs/microservices';
import { createColumnDto } from './dto/createColumn.dto';
import { createRowDto } from './dto/createRow.dto';

@Controller('dynamic-table')
export class AppController {
  constructor(
    private readonly appService: AppService,
    @Inject('KAFKA_SERVICE')
    private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    this.kafkaClient.subscribeToResponseOf('table-events');
    await this.kafkaClient.connect();
    console.log('kafka connected...');
  }

  @Post('create-table')
  async createTable() {
    const event = { action: 'createTable' };
    this.kafkaClient.emit('table-events', event);

    return { message: 'CreateTable event sent to Kafka' };
  }

  @Post('add-column')
  async addColumn(@Body() columnData: createColumnDto) {
    const { tableId, columnName } = columnData;
    if (!tableId) {
      throw new HttpException('Table ID is required', HttpStatus.BAD_REQUEST);
    }
    if (!columnName) {
      throw new HttpException(
        'Column name is required',
        HttpStatus.BAD_REQUEST,
      );
    }
    const event = { action: 'createColumn',data: columnData }
    this.kafkaClient.emit('table-events', event);
    return { message: 'Create column event sent to Kafka' };
  }

  @Post('add-row')
  async addRow(
    @Body('tableId') tableId: number,
    @Body('rowData') rowData: Record<number, any>,
  ) {
    if (!tableId) {
      throw new HttpException('Table ID is required', HttpStatus.BAD_REQUEST);
    }
    
    const event = { action: 'createRow',data: {tableId,rowData} }
    this.kafkaClient.emit('table-events', event);
    return { message: 'Create data event sent to Kafka' };
  }

  @Get()
  async getTable(@Body('tableId') tableId: number){
    if (!tableId) {
      throw new HttpException('Table ID is required', HttpStatus.BAD_REQUEST);
    }
    try {
      const result = await this.kafkaClient.send('table-events', {
        action: 'getTable',
        data: { tableId },
      }).toPromise(); // Await the response from Kafka
      return result; // Return the response from dynamic-table
    } catch (error) {
      console.error('Error fetching table:', error.message);
      throw new HttpException(
        'Failed to fetch table',
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
